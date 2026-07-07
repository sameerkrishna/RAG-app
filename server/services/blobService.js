import { put, del, list, get } from '@vercel/blob';

/**
 * Uploads a PDF buffer to Vercel Blob.
 */
export async function uploadPdfToStorage(sessionId, documentId, filename, buffer) {
  const filePath = `documents/${sessionId}/${documentId}_${filename}`;
  console.log(`[VercelBlob] Uploading PDF to storage: ${filePath}`);

  const blob = await put(filePath, buffer, {
    access: 'private',
    contentType: 'application/pdf',
  });

  return blob;
}

/**
 * Gets the exact URL for a PDF from Vercel Blob by searching.
 */
export async function getPdfUrlFromStorage(sessionId, documentId, filename) {
  const filePath = `documents/${sessionId}/${documentId}_`; // The filename might have been altered by Vercel Blob, so we search by prefix

  const { blobs } = await list({
    prefix: filePath,
    limit: 1
  });

  if (blobs.length > 0) {
    const blob = blobs[0];
    if (blob.downloadUrl) {
      return blob.downloadUrl;
    }
    return blob.url;
  }

  throw new Error('PDF not found in blob storage');
}

/**
 * Gets a Web Stream for a private PDF from Vercel Blob.
 */
export async function getPdfStreamFromStorage(url) {
  const result = await get(url, {
    access: 'private',
  });
  
  if (!result) {
    throw new Error('PDF not found in blob storage');
  }
  
  return result;
}

/**
 * Deletes a PDF from Vercel Blob.
 */
export async function deletePdfFromStorage(sessionId, documentId, filename, knownUrl = null) {
  if (knownUrl) {
    console.log(`[VercelBlob] Deleting PDF from storage using known URL: ${knownUrl}`);
    await del(knownUrl);
    return true;
  }

  const filePath = `documents/${sessionId}/${documentId}_`;
  console.log(`[VercelBlob] Deleting PDF from storage (searching by prefix): ${filePath}`);

  const { blobs } = await list({
    prefix: filePath,
    limit: 1
  });

  if (blobs.length > 0) {
    const url = blobs[0].url;
    await del(url);
    console.log(`[VercelBlob] Successfully deleted: ${url}`);
    return true;
  }

  console.log(`[VercelBlob] No file found to delete for: ${filePath}`);
  return false;
}
