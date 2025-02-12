FROM arm64v8/centos:7

RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/CentOS-Base.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/CentOS-Base.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/CentOS-Base.repo
RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y libzip unzip wget cmake openssl-devel llvm clang-devel clang krb5-workstation git gcc

# install gcc-8
# Note: devtoolset may not be available for aarch64, you will need to use the default gcc or find an alternative
# RUN yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++
# RUN echo '. /opt/rh/devtoolset-8/enable' >> ~/.bashrc

# install llvm-toolset
# Note: llvm-toolset may not be available for aarch64, you will need to use the default llvm or find an alternative
# RUN yum install -y llvm-toolset-7
# RUN echo '. /opt/rh/llvm-toolset-7/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
# Note: Specify the correct target for aarch64
RUN /rustup-init -y --default-toolchain nightly-2024-10-01-aarch64-unknown-linux-gnu

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
# Note: Update JAVA_HOME path for aarch64
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc

# install protoc
# Note: Download the aarch64 version of protoc
RUN wget -O /protobuf-21.7-linux-aarch64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-aarch_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-aarch64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

# attach libjvm.so
# Note: Update the library path for aarch64
RUN echo 'export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64/server:${LD_LIBRARY_PATH}' >> ~/.bashrc

# setup hadoop env
RUN curl -LsSf https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz | tar zxf - -C /root
RUN echo "export HADOOP_HOME=/root/hadoop-3.3.5" >> ~/.bashrc
RUN echo "export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)" >> ~/.bashrc
RUN echo "export HDRS_NAMENODE=default" >> ~/.bashrc
RUN echo "export HDRS_WORKDIR=/tmp/hdrs/" >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc

# ref https://github.com/lancedb/lance/pull/1462
RUN echo "export CFLAGS_aarch64_unknown_linux_gnu=\"-D__ARM_ARCH=8\"" >> ~/.bashrc

RUN echo "export RUSTFLAGS=\"$RUSTFLAGS -Clinker=clang -C extra-filename=-std=c99\"" >> ~/.bashrc

#
#
#FROM centos:7
#
## install common tools
#RUN echo "sslverify=false" >> /etc/yum.conf
#RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
#RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
#RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
#RUN yum update -y
#RUN yum install -y centos-release-scl epel-release
#RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
#RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
#RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
#RUN yum install -y libzip unzip wget cmake3 openssl-devel llvm clang-devel clang krb5-workstation clang-devel git gcc
#
## install gcc-11
#RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
#RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc
#
#RUN yum install -y llvm-toolset-7
#RUN echo '. /opt/rh/llvm-toolset-7/enable' >> ~/.bashrc
#
## install rust nightly toolchain
#RUN curl https://sh.rustup.rs > /rustup-init
#RUN chmod +x /rustup-init
#RUN /rustup-init -y --default-toolchain nightly-2024-10-01-aarch64-unknown-linux-gnu
#
## install protoc
#RUN wget -O /protobuf-21.7-linux-aarch64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-aarch64.zip
#RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-aarch64.zip)
#RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc
#
#RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc
