import { put, del } from '@vercel/blob';

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
 * Gets the public URL for a PDF from Vercel Blob.
 */
export function getPdfUrlFromStorage(sessionId, documentId, filename) {
  const filePath = `documents/${sessionId}/${documentId}_${filename}`;
  
  // Construct the Vercel Blob public URL
  const storeId = process.env.BLOB_STORE_ID;
  const url = `https://${storeId}.public.blob.vercel-storage.com/${filePath}`;
  
  return url;
}

/**
 * Deletes a PDF from Vercel Blob.
 */
export async function deletePdfFromStorage(sessionId, documentId, filename) {
  const filePath = `documents/${sessionId}/${documentId}_${filename}`;
  console.log(`[VercelBlob] Deleting PDF from storage: ${filePath}`);

  const storeId = process.env.BLOB_STORE_ID;
  const url = `https://${storeId}.public.blob.vercel-storage.com/${filePath}`;

  await del(url);
  return true;
}
