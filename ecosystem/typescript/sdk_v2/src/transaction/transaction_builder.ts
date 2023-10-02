import { AptosConfig } from "../api/aptos_config";
import { Account } from "../core";
import { AptosRawTransaction, HexInput, TransactionData, TransactionOptions, TransactionPayload } from "../types";

export const TransactionBuilder = {
  /**
   * Builds a transaction payload based on the data argument and returns
   * entry function payload or script payload
   */
  generateTransactionPayload: async (args: {
    aptosConfig: AptosConfig;
    data: TransactionData;
  }): Promise<TransactionPayload> => {
    const { aptosConfig, data } = args;
    if ("code" in data) {
      // generate script payload
    } else if (data.arguments[0] instanceof Uint8Array) {
      // TODO maybe something more sophisticate? loop all arguments and if something is not Uint8Array
      // insert into needSerialization array and serialize it based on the remote abi
      // generate entry function payload
    } else {
      // generate entry function payload with remote abi
    }
  },

  /**
   * Generates a raw transaction with the TransactionPayload (returned from generateTransactionPayload)
   * and based on the provided optional arguments secondarySigners and/or feePayer.
   */
  generateRawTransaction: async (args: {
    aptosConfig: AptosConfig;
    sender: HexInput;
    payload: TransactionPayload;
    options?: TransactionOptions;
    secondarySigners?: Array<HexInput>;
    feePayer?: HexInput;
  }): Promise<AptosRawTransaction> => {
    const { aptosConfig, sender, payload, options, secondarySigners, feePayer } = args;
    if (feePayer) {
      // generate fee payer raw transaction
    } else if (secondarySigners) {
      // generate multi agent raw transaction
    }
    // generate raw transaction
  },

  /**
   * Signs a single signer transaction that can be submitted to chain
   */
  signSingleSignerTransaction: (args: {
    aptosConfig: AptosConfig;
    signer: Account;
    transaction: AptosRawTransaction;
  }): Uint8Array => {},

  /**
   * Signs a multi signers transaction that can later be used to
   * build a transaction authenticator that can be submitted to chain
   */
  signMultiSignersTransaction: (args: {
    aptosConfig: AptosConfig;
    signer: Account;
    transaction: AptosRawTransaction;
  }): AccountAuthenticator => {},

  /**
   * Create a transaction authenticator built from all the account authenticators
   * (see signMultiSignersTransaction) and then build a bcs serialized transaction
   * that can be submitted to chain
   */
  prepareMultiSignersTransaction: (args: {
    transaction: AptosRawTransaction;
    authenticators: Array<Authenticators>;
  }): Uint8Array => {},
};
