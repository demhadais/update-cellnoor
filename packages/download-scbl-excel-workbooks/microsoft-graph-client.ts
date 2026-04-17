import { ClientSecretCredential } from "@azure/identity";
import { TokenCredentialAuthenticationProvider } from "@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials";
import { Client } from "@microsoft/microsoft-graph-client";
import type {
  ConfigValidator,
  readConfig,
  WorkbookSpecification,
} from "./config";

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

export async function downloadWorkbook(
  client: Client,
  { siteId, filePath }: { siteId: string; filePath: string },
): Promise<ReadableStream> {
  const file = await client
    .api(`/sites/${siteId}/drive/root:/${filePath}`)
    .get();

  return await client.api(file["@microsoft.graph.downloadUrl"]).get();
}
