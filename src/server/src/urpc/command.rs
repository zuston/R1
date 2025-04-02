use crate::app::{
    AppManagerRef, PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
    WritingViewContext,
};
use crate::constant::StatusCode;
use crate::metric::URPC_SEND_DATA_TRANSPORT_TIME;
use crate::store::ResponseDataIndex::Local;
use crate::store::{Block, LocalDataIndex, ResponseData};
use crate::urpc::connection::Connection;
use crate::urpc::frame::Frame;
use crate::urpc::shutdown::Shutdown;
use crate::util;
use anyhow::Result;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use log::{debug, error};
use std::collections::HashMap;

#[derive(Debug)]
pub enum Command {
    Send(SendDataRequestCommand),
    GetMem(GetMemoryDataRequestCommand),
    GetLocalIndex(GetLocalDataIndexRequestCommand),
    GetLocalData(GetLocalDataRequestCommand),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        match frame {
            Frame::SendShuffleData(req) => Ok(Command::Send(req)),
            Frame::GetMemoryData(req) => Ok(Command::GetMem(req)),
            Frame::GetLocalDataIndex(req) => Ok(Command::GetLocalIndex(req)),
            Frame::GetLocalData(req) => Ok(Command::GetLocalData(req)),
            _ => todo!(),
        }
    }

    pub async fn apply(
        self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        match self {
            Command::Send(req) => req.apply(app_manager_ref, conn, shutdown).await?,
            Command::GetMem(req) => req.apply(app_manager_ref, conn, shutdown).await?,
            Command::GetLocalIndex(req) => req.apply(app_manager_ref, conn, shutdown).await?,
            Command::GetLocalData(req) => req.apply(app_manager_ref, conn, shutdown).await?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GetMemoryDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) last_block_id: i64,
    pub(crate) read_buffer_size: i32,
    pub(crate) expected_tasks_bitmap_raw: Option<Bytes>,
    pub(crate) timestamp: i64,
}

impl GetMemoryDataRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let last_block_id = self.last_block_id;
        let read_buffer_size = self.read_buffer_size;

        let app = app_manager_ref.get_app(&app_id);
        if app.is_none() {
            let response = RpcResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
            };
            write_response(conn, response).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
        let ctx = ReadingViewContext {
            uid,
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                read_buffer_size as i64,
            ),
            serialized_expected_task_ids_bitmap: None,
        };

        let response = match app.select(ctx).await {
            Err(e) => GetMemoryDataResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on getting memory data. err: {:#?}", e),
                data: ResponseData::Mem(Default::default()),
            },
            Ok(res) => GetMemoryDataResponseCommand {
                request_id,
                status_code: StatusCode::SUCCESS.into(),
                ret_msg: "".to_string(),
                data: res,
            },
        };
        let frame = Frame::GetMemoryDataResponse(response);
        conn.write_frame(&frame).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetLocalDataResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data: Bytes,
}

#[derive(Debug, Default)]
pub struct GetLocalDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
    pub(crate) offset: i64,
    pub(crate) length: i32,
    pub(crate) timestamp: i64,
}

impl GetLocalDataRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let offset = self.offset;
        let length = self.length;

        let app = app_manager_ref.get_app(&app_id);
        if app.is_none() {
            let command = GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data: Default::default(),
            };
            let frame = Frame::GetLocalDataResponse(command);
            conn.write_frame(&frame)
                .instrument_await("No such app and then fast return")
                .await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
        let ctx = ReadingViewContext {
            uid,
            reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
            serialized_expected_task_ids_bitmap: None,
        };
        let command = match app
            .select(ctx)
            .instrument_await(format!("getting local shuffle data for app:{}", &app_id))
            .await
        {
            Err(e) => GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on getting file data. err: {:#?}", e),
                data: Default::default(),
            },
            Ok(result) => {
                if let ResponseData::Local(data) = result {
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::SUCCESS.into(),
                        ret_msg: "".to_string(),
                        data: data.data,
                    }
                } else {
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: "Incorrect local data type.".to_string(),
                        data: Default::default(),
                    }
                }
            }
        };

        let frame = Frame::GetLocalDataResponse(command);
        conn.write_frame(&frame).await?;
        return Ok(());
    }
}

#[derive(Debug)]
pub struct GetLocalDataIndexResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data_index: LocalDataIndex,
}

#[derive(Debug)]
pub struct GetLocalDataIndexRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
}

impl GetLocalDataIndexRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;

        let app = app_manager_ref.get_app(&app_id);
        if app.is_none() {
            let command = GetLocalDataIndexResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data_index: Default::default(),
            };
            let frame = Frame::GetLocalDataIndexResponse(command);
            conn.write_frame(&frame).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
        let ctx = ReadingIndexViewContext { partition_id: uid };

        let command = match app
            .list_index(ctx)
            .instrument_await(format!("listing localfile index for app:{}", &app_id))
            .await
        {
            Err(err) => GetLocalDataIndexResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on listing local index. err: {:#?}", err),
                data_index: Default::default(),
            },
            Ok(index) => {
                let Local(result) = index;
                GetLocalDataIndexResponseCommand {
                    request_id,
                    status_code: StatusCode::SUCCESS.into(),
                    ret_msg: "".to_string(),
                    data_index: result,
                }
            }
        };
        let frame = Frame::GetLocalDataIndexResponse(command);
        conn.write_frame(&frame).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetMemoryDataResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data: ResponseData,
}

#[derive(Debug, Clone)]
pub struct SendDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) blocks: HashMap<i32, Vec<Block>>,
    pub(crate) ticket_id: i64,
    pub(crate) timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct RpcResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
}

async fn write_response(conn: &mut Connection, command: RpcResponseCommand) -> Result<()> {
    let frame = Frame::RpcResponse(command);
    conn.write_frame(&frame).await
}

impl SendDataRequestCommand {
    async fn apply(
        self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        let request_id = self.request_id;
        let ticket_id = self.ticket_id;
        let shuffle_id = self.shuffle_id;
        let app_id = self.app_id.as_str();
        let timestamp = self.timestamp;

        URPC_SEND_DATA_TRANSPORT_TIME
            .observe(((util::now_timestamp_as_millis() - timestamp as u128) / 1000) as f64);

        let app = app_manager_ref.get_app(&app_id);
        if app.is_none() {
            let response = RpcResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
            };
            write_response(conn, response).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let ticket_len = match app
            .release_ticket(ticket_id)
            .instrument_await(format!("releasing allocated ticket for app:{}", &app_id))
            .await
        {
            Err(e) => {
                let response = RpcResponseCommand {
                    request_id,
                    status_code: StatusCode::INTERNAL_ERROR.into(),
                    ret_msg: "No such ticket id. Maybe it has been out of date".to_string(),
                };
                write_response(conn, response).await?;
                return Ok(());
            }
            Ok(len) => len,
        };

        let mut insert_failure_occur = false;
        let mut insert_failure_message = None;

        let mut insert_len = 0;

        let blocks = self.blocks;
        for block in blocks {
            let partition_id = block.0;
            let partition_blocks = block.1;
            let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
            let ctx = WritingViewContext::new(uid, partition_blocks);
            match app
                .insert(ctx)
                .instrument_await(format!("inserting shuffle data for app:{}", &app_id))
                .await
            {
                Ok(size) => insert_len += size as i64,
                Err(e) => {
                    let msg = format!(
                        "Errors on inserting data for app: {:?}. error:{:#?}",
                        &app_id, e
                    );
                    error!("{}", &msg);
                    insert_failure_occur = true;
                    insert_failure_message = Some(msg);
                }
            }
        }
        let _ = app.move_allocated_used_from_budget(insert_len);
        let unused = ticket_len - insert_len;
        if unused > 0 {
            debug!("Has remaining {} allocated buffer.", &unused);
            let _ = app.dec_allocated_from_budget(unused);
        }

        let response = match insert_failure_occur {
            true => RpcResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: insert_failure_message.unwrap(),
            },
            _ => RpcResponseCommand {
                request_id,
                status_code: StatusCode::SUCCESS.into(),
                ret_msg: "".to_string(),
            },
        };
        write_response(conn, response).await?;
        Ok(())
    }
}
