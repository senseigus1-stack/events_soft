docker-compose down
docker build -t bot-dependencies:latest -f Dockerfile.dependencies .
docker-compose build
docker-compose up
