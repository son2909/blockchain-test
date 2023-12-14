import { protos } from "@aptos-labs/aptos-processor-sdk";
import { Event } from "./entities/event.models";
import {
  ProcessingResult,
  TransactionsProcessor,
  grpcTimestampToDate,
} from "@aptos-labs/aptos-processor-sdk";
import { DataSource, EntityManager } from "typeorm";
import { Account } from "./entities/account.model";

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

      const userTransaction = transaction.user!;

      const events = userTransaction.events!;
      const objects: Event[] = [];
      const accounts: Account[] = [];
      let i = 0;

      const registerDomainKey =
        "0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::RegisterNameEvent";
      const stakeOnAmnisKey =
        "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a";
      const ariesKey =
        "0x9770fa9c725cbd97eb50b2be5f7416efdfd1f1554beb0750d4dae4c64e860da3";
      for (const event of events) {
        // tracking people
        const accountEntity = new Account();
        accountEntity.account_address = `0x${event.key!.accountAddress}`;
        accountEntity.data = event.data!;
        accountEntity.type = event.typeStr!;
        accountEntity.inserted_at = insertedAt;
        accounts.push(accountEntity);

        // handle events
        const eventEntity = new Event();
        eventEntity.transactionVersion = transactionVersion.toString();
        eventEntity.eventIndex = i.toString();
        eventEntity.sequenceNumber = event.sequenceNumber!.toString();
        eventEntity.creationNumber = event.key!.creationNumber!.toString();
        eventEntity.accountAddress = `0x${event.key!.accountAddress}`;
        eventEntity.type = event.typeStr!;
        eventEntity.data = event.data!;
        eventEntity.transactionBlockHeight = transactionBlockHeight.toString();
        eventEntity.inserted_at = insertedAt;
        eventEntity.inserted_at = insertedAt;
        eventEntity.event_type = "";
        if (event.typeStr?.includes(registerDomainKey)) {
          eventEntity.event_type = "REGISTER_DOMAIN";
          objects.push(eventEntity);
        }
        if (event.typeStr?.includes(stakeOnAmnisKey)) {
          eventEntity.event_type = "STAKE_ON_AMNIS";
          objects.push(eventEntity);
        }
        if (event.typeStr?.includes(ariesKey)) {
          eventEntity.event_type = "ARIES";
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
                ? `'${value.toISOString()}'`
                : `'${value ?? ""}'`;
            });

            return `(${values.join(", ")})`;
          })
          .join(", ");
        await txnManager.query(`
        INSERT INTO accounts (account_address, data, type, inserted_at)
        VALUES ${valuesString}
        ON CONFLICT DO NOTHING;`);
      }
    });
    // Insert events into the DB.
    return dataSource.transaction(async (txnManager) => {
      const chunkSize = 100;
      for (let i = 0; i < allObjects.length; i += chunkSize) {
        const chunk = allObjects.slice(i, i + chunkSize);
        await txnManager.insert(Event, chunk);
      }
      return {
        startVersion,
        endVersion,
      };
    });
  }
}
