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
 * Downloads a PDF buffer from Vercel Blob.
 * Returns a Blob object (from fetch).
 */
export async function downloadPdfFromStorage(sessionId, documentId, filename) {
  const filePath = `documents/${sessionId}/${documentId}_${filename}`;
  console.log(`[VercelBlob] Downloading PDF from storage: ${filePath}`);

  // Construct the Vercel Blob public URL
  const storeId = process.env.BLOB_STORE_ID;
  const url = `https://${storeId}.public.blob.vercel-storage.com/${filePath}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch blob: ${response.statusText}`);
  }

  const blob = await response.blob();
  return blob;
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
