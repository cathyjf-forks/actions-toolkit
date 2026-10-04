import {BlobClient, BlockBlobUploadStreamOptions} from '@azure/storage-blob'
import {TransferProgressEvent} from '@azure/core-http-compat'
import {Readable} from 'node:stream'
import {pipeline} from 'node:stream/promises'
import {
  getUploadChunkSize,
  getConcurrency,
  getUploadChunkTimeout
} from '../shared/config.js'
import * as core from '@actions/core'
import * as crypto from 'crypto'
import * as stream from 'stream'
import {NetworkError} from '../shared/errors.js'

export interface BlobUploadResponse {
  /**
   * The total reported upload size in bytes. Empty if the upload failed
   */
  uploadSize?: number

  /**
   * The SHA256 hash of the uploaded file. Empty if the upload failed
   */
  sha256Hash?: string
}

export async function uploadToBlobStorage(
  authenticatedUploadURL: string,
  uploadStream: Readable,
  contentType: string
): Promise<BlobUploadResponse> {
  let uploadByteCount = 0
  let lastProgressTime = Date.now()
  const abortController = new AbortController()

  const chunkTimer = async (interval: number): Promise<void> =>
    new Promise((resolve, reject) => {
      const timer = setInterval(() => {
        if (Date.now() - lastProgressTime > interval) {
          reject(new Error('Upload progress stalled.'))
        }
      }, interval)

      abortController.signal.addEventListener('abort', () => {
        clearInterval(timer)
        resolve()
      })
    })

  const maxConcurrency = getConcurrency()
  const bufferSize = getUploadChunkSize()
  const blobClient = new BlobClient(authenticatedUploadURL)
  const blockBlobClient = blobClient.getBlockBlobClient()

  core.debug(
    `Uploading artifact to blob storage with maxConcurrency: ${maxConcurrency}, bufferSize: ${bufferSize}, contentType: ${contentType}`
  )

  const uploadCallback = (progress: TransferProgressEvent): void => {
    core.info(`Uploaded bytes ${progress.loadedBytes}`)
    uploadByteCount = progress.loadedBytes
    lastProgressTime = Date.now()
  }

  const options: BlockBlobUploadStreamOptions = {
    blobHTTPHeaders: {blobContentType: contentType},
    onProgress: uploadCallback,
    abortSignal: abortController.signal
  }

  let sha256Hash: string | undefined = undefined
  const blobUploadStream = new stream.PassThrough()
  const hash = crypto.createHash('sha256')
  const inputTransfer = pipeline(
    uploadStream,
    new stream.Transform({
      transform(chunk, _encoding, callback) {
        hash.update(chunk)
        callback(null, chunk)
      }
    }),
    blobUploadStream
  )

  core.info('Beginning upload of artifact content to blob storage')

  try {
    await Promise.race([
      Promise.all([
        inputTransfer,
        blockBlobClient.uploadStream(
          blobUploadStream,
          bufferSize,
          maxConcurrency,
          options
        )
      ]),
      chunkTimer(getUploadChunkTimeout())
    ])
  } catch (error) {
    uploadStream.destroy()
    blobUploadStream.destroy()
    if (NetworkError.isNetworkErrorCode(error?.code)) {
      throw new NetworkError(error?.code)
    }
    throw error
  } finally {
    abortController.abort()
  }

  core.info('Finished uploading artifact content to blob storage!')

  sha256Hash = hash.digest('hex')
  core.info(`SHA256 digest of uploaded artifact is ${sha256Hash}`)

  if (uploadByteCount === 0) {
    core.warning(
      `No data was uploaded to blob storage. Reported upload byte count is 0.`
    )
  }
  return {
    uploadSize: uploadByteCount,
    sha256Hash
  }
}
