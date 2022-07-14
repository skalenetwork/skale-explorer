# skale-explorer
Service for administrating and running skale blockscouts

### Install and run

#### Prerequisites
- Clone repo
- Install docker-compose (if not installed)
- Put skale-manager ABI for the network in `data/abi.json` file

#### Run

Arguments:
- ENDPOINT - node endpoint with skale-manager _(required)_ 
- SCHAIN_PROXY_DOMAIN - domain of the network proxy _(required)_
- VERSION - version of skalenetwork/blockscout. **latest** by default _(optional)_
- FIRST_SCHAIN_ID - first sChain to handle blockexplorer for _(optional)_
- LAST_SCHAIN_ID - last sChain to handle blockexplorer for _(optional)_

```
ENDPOINT= SCHAIN_PROXY_DOMAIN= run.sh
```