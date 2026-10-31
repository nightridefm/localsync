// @nightride/localsync: the localsync engine's client. SyncClient speaks the
// protocol and applies frames to any SQLite handle; localsyncTransport plugs
// it into localdb's writer worker (see localdb.ts).
export { Applier, type ApplyResult, type Db, STATE_DDL } from './apply.ts'
export {
	type CloseDetail,
	type ConnMode,
	type Run,
	SyncClient,
	type SyncClientOptions,
	type SyncState,
	type Want,
	type WantError,
} from './client.ts'
export {
	type ColumnDesc,
	decodeFrame,
	type Kind,
	rowHash,
	type SqlValue,
	type UnitDesc,
} from './codec.ts'
export {
	type LocalsyncOptions,
	localsyncTransport,
	type TransportContext,
	type TransportHandle,
	type TransportStatus,
} from './localdb.ts'
