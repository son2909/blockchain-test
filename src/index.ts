import { program } from "commander";
import { Config, Worker } from "@aptos-labs/aptos-processor-sdk";
import { EventProcessor } from "./processor";
import { Event } from "./entities/event.models";
import { Account } from "./entities/account.model";

type Args = {
  config: string;
  perf: number;
};


let config = {} as any;
// process.on("exit", async () => {
//   console.log("🚀 ~ file: index.ts:15 ~ config:", config)
//   await new Promise((resolve) => setTimeout(resolve, 5000));
//   await main(config);
// });

program
  .command("process")
  .requiredOption("--config <config>", "Path to a yaml config file")
  .action(async (args: Args) => {
    const { config: configPath } = args
    config = Config.from_yaml_file(configPath);
    await main(config);
  });

async function main(config: any) {
  const processor = new EventProcessor();
  const worker = new Worker({
    config,
    processor,
    models: [Event, Account],
  });
  await worker.run();
}

program.parse();
