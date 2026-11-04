import { ClientSecretCredential } from "@azure/identity";
import { TokenCredentialAuthenticationProvider } from "@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials";
import { Client } from "@microsoft/microsoft-graph-client";

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

type GraphUser = {
  id: string;
  mail: string | null;
  userPrincipalName: string;
  userType: string | null;
};

// Returns a map from lowercase email to OID. Guests are excluded because a
// guest's OID in this tenant differs from the OID in their home tenant
export async function listMemberOids(
  client: Client,
): Promise<Map<string, string>> {
  const oids = new Map<string, string>();

  let page = await client
    .api("/users")
    .select("id,mail,userPrincipalName,userType")
    .top(999)
    .get();

  while (true) {
    for (const user of page.value as GraphUser[]) {
      if (user.userType !== "Member") {
        continue;
      }
      if (user.mail) {
        oids.set(user.mail.toLowerCase(), user.id);
      }
      oids.set(user.userPrincipalName.toLowerCase(), user.id);
    }

    const nextLink = page["@odata.nextLink"];
    if (!nextLink) {
      return oids;
    }
    page = await client.api(nextLink).get();
  }
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
