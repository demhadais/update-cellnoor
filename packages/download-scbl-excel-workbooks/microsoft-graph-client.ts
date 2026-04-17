import { ClientSecretCredential } from "@azure/identity";
import { TokenCredentialAuthenticationProvider } from "@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials";
import { Client } from "@microsoft/microsoft-graph-client";
import type { Config, DataSources, readConfig } from "./config";

export function createMicrosoftGraphClient({
  tenantId,
  clientId,
  clientSecret,
}: {
  tenantId: string;
  clientId: string;
  clientSecret: string;
}) {
  const credential = new ClientSecretCredential(
    tenantId,
    clientId,
    clientSecret,
  );

  const authProvider = new TokenCredentialAuthenticationProvider(credential, {
    scopes: ["https://graph.microsoft.com/.default"],
  });

  return Client.initWithMiddleware({ authProvider });
}

export async function downloadFiles(
  client: Client,
  { data_sources, microsoft_sharepoint_site_id }: Config,
) {
  const downloads = Object.entries(data_sources).map(
    async ([filePath, sheetNames]) => {
      return {
        rawFile: await downloadFile(client, {
          siteId: microsoft_sharepoint_site_id,
          filePath,
        }),
        sheetNames,
      };
    },
  );

  return await Promise.all(downloads);
}

async function downloadFile(
  client: Client,
  { siteId, filePath }: { siteId: string; filePath: string },
): Promise<ReadableStream> {
  const file = await client
    .api(`/sites/${siteId}/drive/root:/${filePath}`)
    .get();

  return await client.api(file["@microsoft.graph.downloadUrl"]).get();
}
