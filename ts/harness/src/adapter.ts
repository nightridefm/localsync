// What a sync engine must provide to be tested. Capabilities only: the
// harness never looks at protocol messages, only at the client replica.
import type { Catalog } from './catalog.ts'
import type { Db } from './pg.ts'
import type { Replica } from './replica.ts'
import type { Bound, Principal } from './rules.ts'

/** The engine cannot express this at all; reported as `unsupported`. */
export class Unsupported extends Error {}

/** A committed write, as an app would report it to its client. */
export type Marker = { xid: string; lsn: bigint }

export interface Client {
	readonly replica: Replica
	/** Called after every local commit (the moment a UI could re-render). */
	onCommit(cb: () => void): () => void
	/** Replace the wanted set. Resolves once the new set is being synced. */
	want(bounds: Bound[]): Promise<void>
	disconnect(): Promise<void>
	reconnect(): Promise<void>
	/** Payload bytes received so far. */
	bytes(): number
	/** Errors the client hit (apply failures etc.), for failure messages. */
	errors(): string[]
	/**
	 * The client says it is caught up on every want the server serves (the
	 * moment an app would stop showing "loading"). Absent: no such signal.
	 */
	caughtUp?(): boolean
	/** Wants the server does not serve now: refused (permanent) or failing. */
	failedWants?(): Array<{ rule: string; error: string; permanent: boolean }>
	close(): Promise<void>
}

export interface Adapter {
	readonly name: string
	connect(principal: Principal, bounds: Bound[], catalog: Catalog): Promise<Client>
	restartServer?(): Promise<void>
	/** LIKE pattern of the replication slots the engine streams from. */
	readonly replicationSlots?: string
	/** Make every existing client cursor too old to resume from. */
	expireRetention?(): Promise<void>
	/**
	 * Throws unless forged, expired and wrong-audience tokens are all
	 * rejected. Absent: the engine has no client auth of its own.
	 */
	tokenCheck?(): Promise<string>
	/** A rule a later connect or want may use (engines that need rules up front). */
	registerRule?(rule: import('./rules.ts').Rule): void
	/** Called when the fixture schema was recreated. */
	schemaReset?(db: Db): Promise<void>
	close(): Promise<void>
}
