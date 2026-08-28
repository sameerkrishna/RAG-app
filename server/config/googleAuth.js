const GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS';

/**
 * Preserve normal Google Application Default Credentials behavior when the
 * environment variable contains a file path. On Vercel, the same variable can
 * contain the complete service-account JSON instead, so no credential file has
 * to exist in the repository or function bundle.
 */
export function getGoogleAuthOptions() {
  const value = process.env[GOOGLE_APPLICATION_CREDENTIALS]?.trim();

  if (!value || !value.startsWith('{')) {
    return {};
  }

  let credentials;
  try {
    credentials = JSON.parse(value);
  } catch {
    throw new Error(
      `${GOOGLE_APPLICATION_CREDENTIALS} must be either a credential file path or valid service-account JSON.`
    );
  }

  const requiredFields = ['client_email', 'private_key'];
  const missingFields = requiredFields.filter(field => !credentials?.[field]);
  if (missingFields.length > 0) {
    throw new Error(
      `${GOOGLE_APPLICATION_CREDENTIALS} JSON is missing required field(s): ${missingFields.join(', ')}.`
    );
  }

  return {
    googleAuthOptions: {
      credentials: {
        ...credentials,
        private_key: String(credentials.private_key).replace(/\\n/g, '\n')
      }
    }
  };
}

