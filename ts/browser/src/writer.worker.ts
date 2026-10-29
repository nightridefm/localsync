// The app's writer worker entry: localdb's writer, with the localsync
// transport registered (the integration documented in localsync's
// ts/transport/src/localdb.ts).
import { localsyncTransport } from '@nightride/localsync'

import { registerSyncTransport } from '../../../../localdb/src/worker/writer.worker.ts'

registerSyncTransport('localsync', localsyncTransport)
