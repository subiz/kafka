## Quick kafka image for testing purpose, not optimized for production use
### What it does ?
It simply runs one node kafka and zookeeper inside one container (based on shopify/kafka)

Auto topic creation is setted to true, there is no need to set advertised host, however advertised port is required

### Build it
`docker build -t thanhpk/kafka:subiz-dev .`

### Run it
`docker run -e "ADVERTISED_PORT=9092" thanhpk/kafka:subiz-dev`

### Git Repo
`bitbucket.org/subiz/kafka/container`
