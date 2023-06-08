import { create } from 'ipfs-core'
import { CID } from 'multiformats/cid'
import { Octokit } from '@octokit/core'
import axios from 'axios'
import { Blob, Buffer } from 'buffer'
import Papa from 'papaparse'

import { delegatedPeerRouting } from '@libp2p/delegated-peer-routing'
import { delegatedContentRouting} from '@libp2p/delegated-content-routing'
import { kadDHT } from '@libp2p/kad-dht'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { create as createIpfsHttpClient } from 'kubo-rpc-client'

import { multiaddr } from '@multiformats/multiaddr'
import { webSockets } from '@libp2p/websockets'

// Define "source of thruth" github repo and conventions
const REPO = 'filecoin-renewables-purchases'
const REPO_OWNER = 'protocol'
const TRANSACTION_FOLDER = '_transaction_'
const STEP_2_FILE_NAME_SUFFIX = '_step2_orderSupply.csv'
const STEP_3_FILE_NAME_SUFFIX = '_step3_match.csv'
const STEP_5_FILE_NAME_SUFFIX = '_step5_redemption_information.csv'
const STEP_6_FILE_NAME_SUFFIX = '_step6_generationRecords.csv'
const STEP_7_FILE_NAME_SUFFIX = '_step7_certificate_to_contract.csv'

const ws = new webSockets()

const ipfsClient = createIpfsHttpClient({
	protocol: 'https',
	port: 5002,
	host: 'green.filecoin.space'
})

// Init Octokit
const octokit = new Octokit({
	auth: `${process.env.github_personal_access_token}`
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
	repo = REPO
	repoOwner = REPO_OWNER
	transactionFolder = TRANSACTION_FOLDER
	contractsFileNameSuffix = STEP_2_FILE_NAME_SUFFIX
	allocationsFileNameSuffix = STEP_3_FILE_NAME_SUFFIX
	redemptionsFileNameSuffix = STEP_5_FILE_NAME_SUFFIX
	attestationsFileNameSuffix = STEP_6_FILE_NAME_SUFFIX
	certificatesFileNameSuffix = STEP_7_FILE_NAME_SUFFIX

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
		if(options.repoOwner != undefined)
			this.repoOwner = options.repoOwner
		if(options.repo != undefined)
			this.repo = options.repo
		if(options.transactionFolder != undefined)
			this.transactionFolder = options.transactionFolder
		if(options.contractsFileNameSuffix != undefined)
			this.contractsFileNameSuffix = options.contractsFileNameSuffix
		if(options.allocationsFileNameSuffix != undefined)
			this.allocationsFileNameSuffix = options.allocationsFileNameSuffix
		if(options.redemptionsFileNameSuffix != undefined)
			this.redemptionsFileNameSuffix = options.redemptionsFileNameSuffix
		if(options.attestationsFileNameSuffix != undefined)
			this.attestationsFileNameSuffix = options.attestationsFileNameSuffix
		if(options.certificatesFileNameSuffix != undefined)
			this.certificatesFileNameSuffix = options.certificatesFileNameSuffix
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

	// Get contents of base repo directory
	async getGithubRepoContents() {
		const repoItems = await octokit.request('GET /repos/{owner}/{repo}/contents', {
			owner: this.repoOwner,
			repo: this.repo
		})
		if(repoItems.status != 200)
			return new Promise((resolve, reject) => {
				reject(repoItems)
			})
		return new Promise((resolve, reject) => {
			resolve(repoItems)
		})
	}

	// Get all transactions folders from Github repo
	async getTransactionsFolders() {
		let repoItems
		try {
			repoItems = await this.getGithubRepoContents()
		} catch (error) {
			return new Promise((resolve, reject) => {
				reject(error)
			})
		}
		// Search through the base repo directory for folders containing TRANSACTION_FOLDER in its name
		return new Promise((resolve, reject) => {
			resolve(repoItems.data.filter((item) => {
				return item.name.indexOf(this.transactionFolder) > -1
					&& item.type == 'dir'
			}))
		})
	}

	// Get content from URI
	async getUriContent(getUri, headers, responseType) {
		return axios(getUri, {
			method: 'get',
			headers: headers,
			responseType: responseType
		})
	}

	unicodeDecodeB64(str) {
		return decodeURIComponent(atob(str));
	}

	// Get raw content from Github repo
	async getRawFromGithub(path, fileName, type, contentType) {		
		const uri = `https://api.github.com/repos/${this.repoOwner}/${this.repo}/contents/${path}/${fileName}`
		const headers = {
			'Authorization': `Bearer ${process.env.github_personal_access_token}`,
			'X-GitHub-Api-Version': '2022-11-28'
		}

		let responseType
		switch (type) {
			case 'arraybuffer':
				responseType = 'arraybuffer'
				break
			default:
				responseType = null
				break
		}

		const resp = await this.getUriContent(uri, headers, responseType)
		if(resp.status != 200)
			return new Promise((resolve, reject) => {
				reject(resp)
			})

		switch (type) {
			case 'csv':
				const csv = this.unicodeDecodeB64(resp.data.content)
				let rows = []
				return new Promise((resolve, reject) => {
					Papa.parse(csv, {
						worker: true,
						header: true,
						dynamicTyping: true,
						comments: "#",
						step: (row) => {
							rows.push(row.data)
						},
						complete: () => {
							resolve(rows)
						}
					})
				})
			case 'arraybuffer':
				return new Promise((resolve, reject) => {
					let content = []
					content.push(resp.data.content)
//					const blob = new Blob(content, {type: contentType})
					const blob = new Buffer.from(content)
					resolve(blob)
				})
			default:
				return new Promise((resolve, reject) => {
					resolve(resp.data.content)
				})
		}
	}

	async getContractsAndAllocationsFromGithub() {
		let contracts = {}
		let allocations = {}
		let transactions = {}
	
		let transactionFolders 
		try {
			transactionFolders = await this.getTransactionsFolders()
		} catch (error) {
			return new Promise((resolve, reject) => {
				reject(error)
			})
		}

		for (const transactionFolder of transactionFolders) {
			// Get contents of transactions directory
			const transactionFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
				owner: this.repoOwner,
				repo: this.repo,
				path: transactionFolder.path
			})
			if(transactionFolderItems.status != 200)
				return new Promise((resolve, reject) => {
					reject(transactionFolderItems)
				})
	
			// Search for contracts CSV file
			const contractsCsvFileName = transactionFolder.name + this.contractsFileNameSuffix
	
			// Check if contracts CSV file is present in the folder
			const contractsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == contractsCsvFileName
					&& item.type == 'file'
			})
	
			if(contractsCsvFile.length != 1)	// not valid TRANSACTION_FOLDER
				continue

			// Get CSV content (contracts for this specific order)
			contracts[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, contractsCsvFileName, 'csv')
			
			// Search through allocations CSV file for match
			const allocationsCsvFileName = transactionFolder.name + this.allocationsFileNameSuffix

			// Check if CSV file is present in the folder
			const allocationsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == allocationsCsvFileName
					&& item.type == 'file'
			})

			if(allocationsCsvFile.length != 1)	// No allocation file found
				continue

			// Get CSV content (allocations for this specific order)
			allocations[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, allocationsCsvFileName, 'csv')

			// Delete mutable columns and at same create DAG structures for allocations
			for (let allocation of allocations[transactionFolder.name]) {
				// Check if there is valid CSV line
				if(!allocation.contract_id)
					continue
				// Delete mutable columns
				delete allocation.step4_ZL_contract_complete
				delete allocation.step5_redemption_data_complete
				delete allocation.step6_attestation_info_complete
				delete allocation.step7_certificates_matched_to_supply
				delete allocation.step8_IPLDrecord_complete
				delete allocation.step9_transaction_complete
				delete allocation.step10_volta_complete
				delete allocation.step11_finalRecord_complete

				// Make sure MWh are Numbers
				if(typeof allocation.volume_MWh == "string") {
					allocation.volume_MWh = allocation.volume_MWh.replace(',', '')
					allocation.volume_MWh = allocation.volume_MWh.trim()
					allocation.volume_MWh = Number(allocation.volume_MWh)
				}
			}

			// Delete mutable columns and at same create DAG structures for contracts
			for (let contract of contracts[transactionFolder.name]) {
				// Delete mutable columns
				delete contract.step2_order_complete
				delete contract.step3_match_complete
				delete contract.step4_ZL_contract_complete
				delete contract.step5_redemption_data_complete
				delete contract.step6_attestation_info_complete
				delete contract.step7_certificates_matched_to_supply
				delete contract.step8_IPLDrecord_complete
				delete contract.step9_transaction_complete
				delete contract.step10_volta_complete
				delete contract.step11_finalRecord_complete

				// Make sure MWh are Numbers
				if(typeof contract.volume_MWh == "string") {
					contract.volume_MWh = contract.volume_MWh.replace(',', '')
					contract.volume_MWh = contract.volume_MWh.trim()
					contract.volume_MWh = Number(contract.volume_MWh)
				}
				
				// Add links to demands
				contract.allocations = allocations[transactionFolder.name].filter((allocation)=>{
					return allocation.contract_id == contract.contract_id
				})
			}

			// Create transaction object
			const transaction = {
				name: transactionFolder.name,
				contracts: contracts[transactionFolder.name],
				allocations: allocations[transactionFolder.name]
			}

			// Add allocations to transaction object for this transaction
			transactions[transactionFolder.name] = transaction
		}
		return new Promise((resolve, reject) => {
			resolve(transactions)
		})
	}

	async getAttestationsAndCertificatesFromGithub() {
		let redemptions = {}
		let attestations = {}
		let certificates = {}
		let deliveries = {}

		let transactionFolders 
		try {
			transactionFolders = await this.getTransactionsFolders()
		} catch (error) {
			return new Promise((resolve, reject) => {
				reject(error)
			})
		}

		for (const transactionFolder of transactionFolders) {	
			// Get contents of transactions directory
			const transactionFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
				owner: this.repoOwner,
				repo: this.repo,
				path: transactionFolder.path
			})
			if(transactionFolderItems.status != 200)
				return new Promise((resolve, reject) => {
					reject(transactionFolderItems)
				})

			// Search for redemption CSV file
			const redemptionsCsvFileName = transactionFolder.name + this.redemptionsFileNameSuffix
	
			// Check if CSV file is present in the folder
			const redemptionsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == redemptionsCsvFileName
					&& item.type == 'file'
			})
	
			if(redemptionsCsvFile.length != 1)
				continue

			// Get CSV content (redemptions for this specific order)
			redemptions[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, redemptionsCsvFileName, 'csv')

			// Theoretically we should search folder path with each attestation
			// but since all redemptions point to the same folder let take it from line 1
			if(redemptions[transactionFolder.name].length == 0) {
				console.error(`Empty redemptions file ${redemptionsCsvFileName}`)
				continue
			}

			const attestationFolder = redemptions[transactionFolder.name][0].attestation_folder
			if(!attestationFolder || !attestationFolder.length) {
				console.error(`Invalid attestation folder specified in ${redemptionsCsvFileName}`)
				continue
			}

			// Look for attestation folder and its contents
			let attestationFolderItems
			try {
				attestationFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
					owner: this.repoOwner,
					repo: this.repo,
					path: attestationFolder
				})
			}
			catch (error) {
				console.error(error)
				continue
			}
			if(attestationFolderItems.status != 200) {
				console.error(attestationFolderItems)
				continue
			}

			// Search for attestations CSV file
			const attestationsCsvFileName = attestationFolder + this.attestationsFileNameSuffix

			// Check if CSV file is present in the folder
			const attestationsCsvFile = attestationFolderItems.data.filter((item) => {
				return item.name == attestationsCsvFileName
					&& item.type == 'file'
			})

			if(attestationsCsvFile.length != 1) {
				console.error(`No attestation file ${attestationsCsvFileName} exists in ${attestationFolder}`)
				continue
			}

			// Get CSV content (acctually attestations and attestations)
			attestations[attestationFolder] = await this.getRawFromGithub(attestationFolder, attestationsCsvFileName, 'csv')

			// Search for match / supply CSV file
			const certificatesCsvFileName = attestationFolder + this.certificatesFileNameSuffix

			// Check if CSV file is present in the folder
			const certificatesCsvFile = attestationFolderItems.data.filter((item) => {
				return item.name == certificatesCsvFileName
					&& item.type == 'file'
			})

			if(certificatesCsvFile.length != 1) {
				console.error(`No certificates file ${certificatesCsvFileName} exists in ${attestationFolder}`)
				continue
			}

			// Get CSV content (certificates)
			certificates[attestationFolder] = await this.getRawFromGithub(attestationFolder, certificatesCsvFileName, 'csv')

			// Itterate over attestations and associate certificates and allocations
			for (let attestation of attestations[attestationFolder]) {
				const allocations = certificates[attestationFolder].filter((certificate)=>{
					return certificate.certificate == attestation.certificate
				})
				attestation.allocations = allocations
			}

			// Create delivery object
			const delivery = {
				name: transactionFolder.name,
				attestations: attestations[attestationFolder],
				"certificate-allocations": certificates[attestationFolder]
			}

			// Add allocations to transaction object for this transaction
			deliveries[transactionFolder.name] = delivery
		}
		return new Promise((resolve, reject) => {
			resolve(deliveries)
		})
	}

	async getAllAllocationsFromGithub() {
		let contracts = {}
		let allocations = {}
		let allAllocations = []
	
		let transactionFolders 
		try {
			transactionFolders = await this.getTransactionsFolders()
		} catch (error) {
			return new Promise((resolve, reject) => {
				reject(error)
			})
		}

		for (const transactionFolder of transactionFolders) {
			// Get contents of transactions directory
			const transactionFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
				owner: this.repoOwner,
				repo: this.repo,
				path: transactionFolder.path
			})
			if(transactionFolderItems.status != 200)
				return new Promise((resolve, reject) => {
					reject(transactionFolderItems)
				})
	
			// Search for contracts CSV file
			const contractsCsvFileName = transactionFolder.name + this.contractsFileNameSuffix
	
			// Check if contracts CSV file is present in the folder
			const contractsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == contractsCsvFileName
					&& item.type == 'file'
			})
	
			if(contractsCsvFile.length != 1)	// not valid TRANSACTION_FOLDER
				continue

			// Get CSV content (contracts for this specific order)
			contracts[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, contractsCsvFileName, 'csv')
			
			// Search through allocations CSV file for match
			const allocationsCsvFileName = transactionFolder.name + this.allocationsFileNameSuffix

			// Check if CSV file is present in the folder
			const allocationsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == allocationsCsvFileName
					&& item.type == 'file'
			})

			if(allocationsCsvFile.length != 1)	// No allocation file found
				continue

			// Get CSV content (allocations for this specific order)
			allocations[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, allocationsCsvFileName, 'csv')

			// Delete mutable columns and at same create DAG structures for allocations
			for (let allocation of allocations[transactionFolder.name]) {
				// Check if there is valid CSV line
				if(!allocation.contract_id)
					continue
				// Delete mutable columns
				delete allocation.step4_ZL_contract_complete
				delete allocation.step5_redemption_data_complete
				delete allocation.step6_attestation_info_complete
				delete allocation.step7_certificates_matched_to_supply
				delete allocation.step8_IPLDrecord_complete
				delete allocation.step9_transaction_complete
				delete allocation.step10_volta_complete
				delete allocation.step11_finalRecord_complete

				// Make sure MWh are Numbers
				if(typeof allocation.volume_MWh == "string") {
					allocation.volume_MWh = allocation.volume_MWh.replace(',', '')
					allocation.volume_MWh = allocation.volume_MWh.trim()
					allocation.volume_MWh = Number(allocation.volume_MWh)
				}
				
				// Add contract fields to the record
				const contractFilter = contracts[transactionFolder.name].filter((c)=>{
					return c.contract_id == allocation.contract_id
				})
				if(contractFilter.length != 1)
					return new Promise((resolve, reject) => {
						reject(`An allocation must refer to exactly one contract. ${allocation.allocation_id} referes to ${contractFilter.length} contracts.`)
					})

				let contract = JSON.parse(JSON.stringify(contractFilter[0]))

				// Delete mutable columns
				delete contract.step2_order_complete
				delete contract.step3_match_complete
				delete contract.step4_ZL_contract_complete
				delete contract.step5_redemption_data_complete
				delete contract.step6_attestation_info_complete
				delete contract.step7_certificates_matched_to_supply
				delete contract.step8_IPLDrecord_complete
				delete contract.step9_transaction_complete
				delete contract.step10_volta_complete
				delete contract.step11_finalRecord_complete

				// Make sure MWh are Numbers
				if(typeof contract.volume_MWh == "string") {
					contract.volume_MWh = contract.volume_MWh.replace(',', '')
					contract.volume_MWh = contract.volume_MWh.trim()
					contract.volume_MWh = Number(contract.volume_MWh)
				}

				// Rename contract volume_MWh and allocation volume_MWh
				delete Object.assign(contract, {contract_volume_MWh: contract.volume_MWh }).volume_MWh
				delete Object.assign(allocation, {allocation_volume_MWh: allocation.volume_MWh }).volume_MWh

				// Itterate over contract object and add its propoerties to allocation object
				allocation = Object.assign(allocation, contract)

				// Add allocation to allAllocations
				allAllocations.push(allocation)
			}
		}
		return new Promise((resolve, reject) => {
			resolve(allAllocations)
		})
	}

	async getAllCertificateAllocationsFromGithub() {
		let redemptions = {}
		let attestations = {}
		let certificates = {}
		let allCertificates = []

		let transactionFolders 
		try {
			transactionFolders = await this.getTransactionsFolders()
		} catch (error) {
			return new Promise((resolve, reject) => {
				reject(error)
			})
		}

		for (const transactionFolder of transactionFolders) {	
			// Get contents of transactions directory
			const transactionFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
				owner: this.repoOwner,
				repo: this.repo,
				path: transactionFolder.path
			})
			if(transactionFolderItems.status != 200)
				return new Promise((resolve, reject) => {
					reject(transactionFolderItems)
				})

			// Search for redemption CSV file
			const redemptionsCsvFileName = transactionFolder.name + this.redemptionsFileNameSuffix
	
			// Check if CSV file is present in the folder
			const redemptionsCsvFile = transactionFolderItems.data.filter((item) => {
				return item.name == redemptionsCsvFileName
					&& item.type == 'file'
			})
	
			if(redemptionsCsvFile.length != 1)
				continue

			// Get CSV content (redemptions for this specific order)
			redemptions[transactionFolder.name] = await this.getRawFromGithub(transactionFolder.path, redemptionsCsvFileName, 'csv')

			// Theoretically we should search folder path with each attestation
			// but since all redemptions point to the same folder let take it from line 1
			if(redemptions[transactionFolder.name].length == 0) {
				console.error(`Empty redemptions file ${redemptionsCsvFileName}`)
				continue
			}

			const attestationFolder = redemptions[transactionFolder.name][0].attestation_folder
			if(!attestationFolder || !attestationFolder.length) {
				console.error(`Invalid attestation folder specified in ${redemptionsCsvFileName}`)
				continue
			}

			// Look for attestation folder and its contents
			let attestationFolderItems
			try {
				attestationFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
					owner: this.repoOwner,
					repo: this.repo,
					path: attestationFolder
				})
			}
			catch (error) {
				console.error(error)
				continue
			}
			if(attestationFolderItems.status != 200) {
				console.error(attestationFolderItems)
				continue
			}

			// Search for attestations CSV file
			const attestationsCsvFileName = attestationFolder + this.attestationsFileNameSuffix

			// Check if CSV file is present in the folder
			const attestationsCsvFile = attestationFolderItems.data.filter((item) => {
				return item.name == attestationsCsvFileName
					&& item.type == 'file'
			})

			if(attestationsCsvFile.length != 1) {
				console.error(`No attestation file ${attestationsCsvFileName} exists in ${attestationFolder}`)
				continue
			}

			// Get CSV content (acctually attestations and attestations)
			attestations[attestationFolder] = await this.getRawFromGithub(attestationFolder, attestationsCsvFileName, 'csv')

			// Search for match / supply CSV file
			const certificatesCsvFileName = attestationFolder + this.certificatesFileNameSuffix

			// Check if CSV file is present in the folder
			const certificatesCsvFile = attestationFolderItems.data.filter((item) => {
				return item.name == certificatesCsvFileName
					&& item.type == 'file'
			})

			if(certificatesCsvFile.length != 1) {
				console.error(`No certificates file ${certificatesCsvFileName} exists in ${attestationFolder}`)
				continue
			}

			// Get CSV content (certificates)
			certificates[attestationFolder] = await this.getRawFromGithub(attestationFolder, certificatesCsvFileName, 'csv')

			// Find appertain attestation record and join it
			for (let certificate of certificates[attestationFolder]) {
				const attestationsFilter = attestations[attestationFolder].filter((a)=>{
					return a.certificate == certificate.certificate
				})
				if(attestationsFilter.length != 1)
					return new Promise((resolve, reject) => {
						reject(`An certificate must be found in attestation. ${certificate.certificate} referes to ${attestationsFilter.length} attestations.`)
					})

				let attestation = JSON.parse(JSON.stringify(attestationsFilter[0]))

				// Add also attestation folder for the output
				attestation.attestation_folder = attestationFolder

				// Itterate over attestation object and add its propoerties to certificate object
				certificate = Object.assign(certificate, attestation)

				// Add allocation to allAllocations
				allCertificates.push(certificate)
			}
		}
		return new Promise((resolve, reject) => {
			resolve(allCertificates)
		})
	}
}