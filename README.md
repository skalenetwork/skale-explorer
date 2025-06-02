# skale-explorer
Service for administrating and running skale blockscouts

### Install

- Clone repo
- Install docker-compose (if not installed)
- Put skale-manager ABI for the network in `data/abi.json` file
- Create .env file 

### Run

#### Default mode

Agent will generate configuration files and run explorers for chains
```
./run.sh
```

#### Update mode

Agent will update configuration files
```
./run.sh --update
```

#### Verify mode

Agent will run contract verification
```
./run.sh --verify
```

#### Stop mode

Agent will stop explorers, all data from explorers will be saved
```
./run.sh --down
```

#### Restart mode

Agent will restart explorer
```
./run.sh --restart
```

### Arguments

To run explorer-admin, `.env` file shoudl be created in the project root directory. Use `template.env` as example. 

- ETH_ENDPOINT - node endpoint with skale-manager _(required)_ 
- PROXY_DOMAIN - domain of the network proxy _(required)_
- SCHAIN_NAMES - list of schain names to operate _(optional)_
- SSL_ENABLED - set https connection for explorers _(optional)_
- INTERNAL_DOMAIN_NAME - domain of the node, to run explorers with https  _(optional)_  
- WALLET_CONNECT_PROJECT_ID - WalletConnect project ID for blockscout frontend _(optional)_
- IS_TESTNET - whether network is testnet _(optional)_
- BLOCKSCOUT_BACKEND_DOCKER_TAG - version of skalenetwork/blockscout container to use _(optional)_
- BLOCKSCOUT_FRONTEND_DOCKER_TAG - version of skalenetwork/blockscout-frontend container to use _(optional)_
- DB_PASSWORD - password for postgres database _(optional)_
- RE_CAPTCHA_SECRET_KEY - private key used on blockscout server side to securely verify that user interactions on your website are performed by humans _(optional)_
- HTTP_ENDPOINT - set to 'true' to use IP endpoints instead of domain endpoints for devnet support _(optional)_
- HOST - override auto-detected public IP address with custom host value _(optional)_

## Additional Documentation

For detailed instructions on how to use the delete transactions script, please refer to the [Delete Last Transactions Script Documentation](scripts/README.md).
