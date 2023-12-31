import { protos } from "@aptos-labs/aptos-processor-sdk";
import { Event } from "./entities/event.models";
import {
  ProcessingResult,
  TransactionsProcessor,
  grpcTimestampToDate,
} from "@aptos-labs/aptos-processor-sdk";
import { DataSource } from "typeorm";
import { Account } from "./entities/account.model";
import { isDateWithinLast7Days } from "./common";
import { eventTypeEnum, trackingKey } from "./constant";

export class EventProcessor extends TransactionsProcessor {
  name(): string {
    return "event_processor";
  }

  async processTransactions({
    transactions,
    startVersion,
    endVersion,
    dataSource,
  }: {
    transactions: protos.aptos.transaction.v1.Transaction[];
    startVersion: bigint;
    endVersion: bigint;
    dataSource: DataSource; // DB connection
  }): Promise<ProcessingResult> {
    let allObjects: Event[] = [];
    let allAccounts: Account[] = [];

    // Process transactions.
    for (const transaction of transactions) {
      // Filter out all transactions that are not User Transactions
      if (
        transaction.type !=
        protos.aptos.transaction.v1.Transaction_TransactionType
          .TRANSACTION_TYPE_USER
      ) {
        continue;
      }

      const transactionVersion = transaction.version!;
      const transactionBlockHeight = transaction.blockHeight!;
      const insertedAt = grpcTimestampToDate(transaction.timestamp!);
      if (!isDateWithinLast7Days(insertedAt) || !transaction.user) {
        continue;
      }

      const userTransaction = transaction.user!;
      const entryFunctionStr =
        userTransaction.request?.payload?.entryFunctionPayload
          ?.entryFunctionIdStr;

      const events = userTransaction.events!;
      const objects: Event[] = [];
      const accounts: Account[] = [];
      let i = 0;

      for (const event of events) {
        const accountAddr = `0x${event.key!.accountAddress}`;
        // tracking people
        const accountEntity = new Account();
        accountEntity.account_address = accountAddr;
        accountEntity.inserted_at = insertedAt;
        accounts.push(accountEntity);

        // handle events
        const eventEntity = new Event();
        eventEntity.transactionVersion = transactionVersion.toString();
        eventEntity.eventIndex = i.toString();
        eventEntity.sequenceNumber = event.sequenceNumber!.toString();
        eventEntity.creationNumber = event.key!.creationNumber!.toString();
        eventEntity.accountAddress = accountAddr;
        eventEntity.type = event.typeStr!;
        eventEntity.data = event.data!;
        eventEntity.transactionBlockHeight = transactionBlockHeight.toString();
        eventEntity.inserted_at = insertedAt;
        eventEntity.inserted_at = insertedAt;
        eventEntity.application = "";

        if (event.typeStr?.includes(trackingKey.registerDomainKey)) {
          eventEntity.application = eventTypeEnum.REGISTER_DOMAIN;
          objects.push(eventEntity);
        }
        if (accountAddr === trackingKey.stakeOnAmnisKey) {
          eventEntity.application = eventTypeEnum.STAKE_ON_AMNIS;
          objects.push(eventEntity);
        }
        if (entryFunctionStr?.includes(trackingKey.ariesKey)) {
          const payload =
            userTransaction.request?.payload?.entryFunctionPayload?.function;
          eventEntity.application = eventTypeEnum.ARIES;
          eventEntity.type = entryFunctionStr;
          eventEntity.accountAddress = payload?.module?.address!;
          objects.push(eventEntity);
        }
        if (
          accountAddr === trackingKey.swapOnLiquidswapKey_v1 ||
          accountAddr === trackingKey.swapOnLiquidswapKey_v2
        ) {
          eventEntity.application = eventTypeEnum.SWAP_ON_LIQUID;
          objects.push(eventEntity);
        }
        if (accountAddr === trackingKey.swapOnPancakeswapKey) {
          eventEntity.application = eventTypeEnum.SWAP_ON_PANCAKESWAP;
          objects.push(eventEntity);
        }
        if (accountAddr === trackingKey.swapOnThalaKey) {
          eventEntity.application = eventTypeEnum.SWAP_ON_THALA;
          objects.push(eventEntity);
        }
        if (accountAddr === trackingKey.liquidityOnMerkleKey) {
          eventEntity.application = eventTypeEnum.LIQUIDITY_ON_MERKLE;
          objects.push(eventEntity);
        }
        i++;
      }

      allObjects = allObjects.concat(objects);
      allAccounts = allAccounts.concat(accounts);
    }

    // Upsert people into the DB.
    await dataSource.transaction(async (txnManager) => {
      const chunkSize = 100;

      for (let i = 0; i < allAccounts.length; i += chunkSize) {
        const chunk = allAccounts.slice(i, i + chunkSize);
        const valuesString = chunk
          .map((account) => {
            const values = Object.values(account).map((value) => {
              return value instanceof Date
                ? `'${value.toUTCString()}'`
                : `'${value ?? ""}'`;
            });

            return `(${values.join(", ")})`;
          })
          .join(", ");
        await txnManager.query(`
        INSERT INTO accounts (account_address, inserted_at)
        VALUES ${valuesString}
        ON CONFLICT DO NOTHING;`);
      }
    });
    // Insert events into the DB.
    return dataSource.transaction(async (txnManager) => {
      const chunkSize = 100;
      for (let i = 0; i < allObjects.length; i += chunkSize) {
        const chunk = allObjects.slice(i, i + chunkSize);
        await txnManager.upsert(Event, chunk, [
          "transactionVersion",
          "eventIndex",
        ]);
      }
      return {
        startVersion,
        endVersion,
      };
    });
  }
}
