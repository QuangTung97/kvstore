PROTOC_VERSION=3.11.4
PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/$PROTOC_ZIP
sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
sudo chmod 755 /usr/local/bin/protoc
sudo unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
sudo chmod -R a=rX /usr/local/include/google
rm -f $PROTOC_ZIP
