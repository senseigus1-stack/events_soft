docker-compose down
docker build --network=host -t bot-dependencies:latest -f Dockerfile.dependencies .
docker-compose build
docker-compose up
