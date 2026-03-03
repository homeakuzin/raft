set -e
EXECUTABLE=hyperraft.linux-amd64
GOOS=linux GOARCH=amd64 go build -o $EXECUTABLE
HOSTS="pureservers-de timeweb-ams sweb"
for host in $HOSTS; do
    echo Deploying to $host;
    rsync $EXECUTABLE $host:hyperraft;
done