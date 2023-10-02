import {
  AptosRawTransaction,
  HexInput,
  PendingTransactionResponse,
  TransactionData,
  TransactionOptions,
  UserTransactionResponse,
} from "../types";
import { TransactionBuilder } from "../transaction/transaction_builder";
import { AptosConfig } from "./aptos_config";
import { Account } from "../core";

export class TransactionSubmission {
  readonly config: AptosConfig;

  // transactions the SDK supports
  // readonly ansBuilder: ANS;

  constructor(config: AptosConfig) {
    this.config = config;
    //this.ansBuilder = new ANS(this.config);
  }

  /**
   * generates any move function transaction by passing in the
   * move function name, move function type arguments, move function arguments
   * `
   * {
   *  function:"0x1::aptos_account::transfer",
   *  type_arguments:[]
   *  arguments:[recieverAddress,10]
   * }
   * `
   */
  async generateMoveTransaction(args: {
    sender: HexInput;
    data: TransactionData;
    options?: TransactionOptions;
  }): Promise<AptosRawTransaction> {
    const { sender, data, options } = args;
    const { secondary_signer_addresses, fee_payer_address } = data;
    const payload = await TransactionBuilder.generateTransactionPayload({ aptosConfig: this.config, data });
    const rawTransaction = await TransactionBuilder.generateRawTransaction({
      aptosConfig: this.config,
      sender,
      payload,
      options,
      secondarySigners: secondary_signer_addresses,
      feePayer: fee_payer_address,
    });
    return rawTransaction;
  }

  /**
   * Sign a single signer transaction
   */
  async signSingleSignerTransaction(args: { signer: Account; transaction: AptosRawTransaction }): Uint8Array {
    return TransactionBuilder.signSingleSignerTransaction({ aptosConfig: this.config, ...args });
  }

  /**
   * Sign a multi signers transaction, i.e fee payer / multi agent
   */
  async signMultiSignersTransaction(): AccountAuthenticator {}

  /**
   * Simulates a transaction
   */
  async simulateTransaction(): UserTransactionResponse {}

  /**
   * Before submitting a multi signer transaction, i.e fee payer / multi agent ,
   * we need to create a transaction authenticator built from all the account authenticators
   * (see signMultiSignersTransaction) and then build a bcs serialized transaction
   * that can be submitted to chain
   */
  async prepareMultiSignersTransaction(): Uint8Array {}

  /**
   * Submit transaction to chain by calling the fullnode endpoint
   */
  async submitTransaction(): Promise<PendingTransactionResponse> {}

  // public get ans() {
  //   return this.ansBuilder;
  // }
}
