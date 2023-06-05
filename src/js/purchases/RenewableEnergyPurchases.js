import { create } from 'ipfs-core'
//import { create as createClient } from 'ipfs-http-client'
import { CID } from 'multiformats/cid'

import { delegatedPeerRouting } from '@libp2p/delegated-peer-routing'
import { delegatedContentRouting} from '@libp2p/delegated-content-routing'
import { kadDHT } from '@libp2p/kad-dht'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { create as createIpfsHttpClient } from 'kubo-rpc-client'

import { multiaddr } from '@multiformats/multiaddr'
import { webSockets } from '@libp2p/websockets'

const ws = new webSockets()
//const ipfsClient = createClient({url: '/dns4/green.filecoin.space/tcp/5002/https', timeout: '1w'})
const ipfsClient = createIpfsHttpClient({
	protocol: 'https',
	port: 5002,
	host: 'green.filecoin.space'
})

export class RenewableEnergyPurchases {
	peers = [
		'/dns4/green.filecoin.space/tcp/5004/wss/p2p/12D3KooWJmYbQp2sgKX22vZgSRVURkpMQ5YCSc8vf3toHesJc5Y9',
		'/dns4/web1.co2.storage/tcp/5004/wss/p2p/12D3KooWCPzmui9TSQQG8HTNcZeFiHz6AGS19aaCwxJdjykVqq7f',
		'/dns4/web2.co2.storage/tcp/5004/wss/p2p/12D3KooWFBCcWEDW9GYr9Aw8D2QL7hZakPAw1DGfeZCwfsrjd43b',
		'/dns4/proxy.co2.storage/tcp/5004/wss/p2p/12D3KooWGWHSrAxr6sznTpdcGuqz6zfQ2Y43PZQzhg22uJmGP9n1',
	]
//	ipfsRepoName = './ipfs_repo_' + Math.random()
	ipfsRepoName = './.ipfs'
	ipfsNodeOpts = {
		config: {
//			dht: {
//				enabled: true,
//			},
Addresses: {
	Delegates: [/*
		'/dns4/green.filecoin.space/tcp/5002/https',
		'/dns4/web1.co2.storage/tcp/5002/https',
		'/dns4/web2.co2.storage/tcp/5002/https',
		'/dns4/proxy.co2.storage/tcp/5002/https',*/
		'/dns4/node0.delegate.ipfs.io/tcp/443/https',
		'/dns4/node1.delegate.ipfs.io/tcp/443/https',
		'/dns4/node2.delegate.ipfs.io/tcp/443/https',
		'/dns4/node3.delegate.ipfs.io/tcp/443/https'
	  ]
},
Bootstrap: [
	'/dns4/green.filecoin.space/tcp/5004/wss/p2p/12D3KooWJmYbQp2sgKX22vZgSRVURkpMQ5YCSc8vf3toHesJc5Y9',
	'/dns4/web1.co2.storage/tcp/5004/wss/p2p/12D3KooWCPzmui9TSQQG8HTNcZeFiHz6AGS19aaCwxJdjykVqq7f',
	'/dns4/web2.co2.storage/tcp/5004/wss/p2p/12D3KooWFBCcWEDW9GYr9Aw8D2QL7hZakPAw1DGfeZCwfsrjd43b',
	'/dns4/proxy.co2.storage/tcp/5004/wss/p2p/12D3KooWGWHSrAxr6sznTpdcGuqz6zfQ2Y43PZQzhg22uJmGP9n1',
]
		},
		libp2p: {
			transports: [ws],
			connectionManager: {
				autoDial: false
			},
			config: {
				dht: {
					enabled: true,
					clientMode: true
				}
			},
			contentRouting: [
				delegatedContentRouting(ipfsClient)
			],
			peerRouting: [
				delegatedPeerRouting(ipfsClient)
			],
			services: {
				dht: kadDHT(),
				pubsub: gossipsub()
			}
		}
	}
    ipfs = null
	ipfsStarting = false
	ipfsStarted = false
	contractsAndAllocationsKey = '/ipns/k51qzi5uqu5dlwhffqq4a8ksdtr14d3vckvhldpuxd68r84g3eqsjqgqdvxazc'
	certificatesAndAttestationsKey = '/ipns/k51qzi5uqu5dkllf259064y4qyr6ra1zk7u8qgigsoahwo04m0efnf88827ume'

	// Constructor
    constructor(options) {
		if(!options)
			return
		if(options.ipfsRepoName != undefined)
			this.ipfsRepoName = options.ipfsRepoName
		if(options.ipfsNodeOpts != undefined)
			this.ipfsNodeOpts = Object.assign(this.ipfsNodeOpts, options.ipfsNodeOpts)
		if(options.contractsAndAllocationsKey != undefined)
			this.contractsAndAllocationsKey = options.contractsAndAllocationsKey
		if(options.certificatesAndAttestationsKey	 != undefined)
			this.certificatesAndAttestationsKey = options.certificatesAndAttestationsKey
	}

	// Simple sleep function
	sleep = ms => new Promise(r => setTimeout(r, ms))

	// Create IPFS node and connect swarm peers
	async startIpfs() {
		const that = this
		this.ipfsStarting = true
		this.ipfsStarted = false

		let ipfsOpts = {}

		ipfsOpts = Object.assign({
			repo: this.ipfsRepoName
		}, this.ipfsNodeOpts)
		this.ipfs = await create(ipfsOpts)

		const config = await this.ipfs.config.getAll()
		const hostPeerId = config.Identity.PeerID
		for (const peer of this.peers) {
			if(peer.indexOf(hostPeerId) == -1)
				try {
					const ma = multiaddr(peer)
					await this.ipfs.bootstrap.add(ma)
					await this.ipfs.swarm.connect(ma)
				} catch (error) {
					console.log(peer, error)
				}
		}
		this.ipfsStarted = true
		this.ipfsStarting = false
		return this.ipfs
	}

	// Stop IPFS node
	async stopIpfs() {
		if(this.ipfs != null) {
			await this.ipfs.stop()
			this.ipfs = null
			this.ipfsStarted = false
			this.ipfsStarting = false
		}
	}

	// Run to ensure IPFS node will start eventually
	async ensureIpfsIsRunning() {
		if(!this.ipfsStarted && !this.ipfsStarting) {
			this.ipfs = await this.startIpfs()
		}
		else if(!this.ipfsStarted) {
			while(!this.ipfsStarted) {
				await this.sleep(1000)
			}
		}
		return this.ipfs
	}

	// Resolve IPNS name/key
	async resolveIpnsName(id) {
		let cid
		for await (const value of await this.ipfs.dht.get(id)) {
			if(!value.error && value.name.toLowerCase() == 'value') {
				const resp = new TextDecoder().decode(value.value)
				const cidStartIndex = resp.indexOf('/ipfs/') + 6
				const  cidEndIndex = cidStartIndex + 59
				cid = resp.substring(cidStartIndex, cidEndIndex)
				break
			}
		}
		return cid
	}

	// Retrieve DAG object from IPNS
	async getDagFromIPNS(id) {
		const hash = (await this.resolveIpnsName(id)).replace('/ipfs/', '')

		// Create CID
		const cid = CID.parse(hash)

		return await this.getDag(cid)
	}
	
	// Retrieve DAG object from the content address
	async getDag(cid) {
		let dag
		// Grab DAG
		dag = await this.ipfs.dag.get(cid, {})

		return {
			cid: cid,
			dag: dag.value
		}
	}

	// Get accolations and contracts objects from IPNS/IPFS
	async getContractsAndAllocations() {
		if(!this.ipfs)
			await this.ensureIpfsIsRunning()

		return (await this.getDag((await this.getDagFromIPNS(this.contractsAndAllocationsKey)).dag.transactions_cid)).dag
	}

	// Get certificates and attestations objects from IPNS/IPFS
	async getCertificatesAndAttestations() {
		if(!this.ipfs)
			await this.ensureIpfsIsRunning()

		return (await this.getDag((await this.getDagFromIPNS(this.certificatesAndAttestationsKey)).dag.deliveries_cid)).dag
	}
}