use crate::app::APP_MANAGER_REF;
use crate::historical_apps::HistoricalAppInfo;
use crate::http::Handler;
use poem::web::Json;
use poem::{handler, RouteMethod};

#[derive(Default)]
pub struct HistoricalAppsHandler;

impl Handler for HistoricalAppsHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(json)
    }

    fn get_route_path(&self) -> String {
        "/historical/apps/json".to_string()
    }
}

#[handler]
fn json() -> Json<Vec<HistoricalAppInfo>> {
    let manager_ref = APP_MANAGER_REF.get().unwrap();
    let mut apps = vec![];
    if let Some(historical_manager) = manager_ref.get_historical_statistics() {
        apps = historical_manager.dump();
    }
    Json(apps)
}
