import { program } from "commander";
import { Config, Worker } from "@aptos-labs/aptos-processor-sdk";
import { EventProcessor } from "./processor";
import { Event } from "./entities/event.models";
import { Account } from "./entities/account.model";
import { getClient } from "./db";

type Args = {
  config: string;
  perf: number;
};

program
  .command("process")
  .requiredOption("--config <config>", "Path to a yaml config file")
  .action(async (args: Args) => {
    const config = Config.from_yaml_file(args.config);

    await main(config);
  });

async function main(config: any) {
  let processor = new EventProcessor();
  let worker = new Worker({
    config,
    processor,
    models: [Event, Account],
  });
  await worker.run();

  setInterval(async () => {
    processor = new EventProcessor();
    const client = await getClient(config?.db_connection_uri);
    const result = await client.query(`Select * from next_version_to_process;`);
    if (result?.rows[0]) {
      config.starting_version = Number(result?.rows[0]?.next_version);
    }
    worker = new Worker({
      config,
      processor,
      models: [Event, Account],
    });
    await worker.run();
  }, 5 * 60 * 1000);
}

program.parse();
