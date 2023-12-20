import { Client } from "pg";
import { parsePostgresUri } from "./common";

export const getClient = async (dbUri: string) => {
  const databaseOptions = parsePostgresUri(dbUri);

  const client = new Client({
    host: databaseOptions?.host,
    port: databaseOptions?.port || 5432,
    user: databaseOptions?.username,
    password: databaseOptions?.password,
    database: databaseOptions?.database,
  });
  await client.connect();
  return client;
};
