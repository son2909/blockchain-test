import { Base } from "@aptos-labs/aptos-processor-sdk";
import { Column, Entity, PrimaryColumn } from "typeorm";

@Entity("accounts")
export class Account extends Base {
  @PrimaryColumn()
  account_address!: string;

  @Column()
  data!: string;

  @Column()
  type!: string;

  @Column({ type: "timestamptz", nullable: true })
  inserted_at!: Date;
}
