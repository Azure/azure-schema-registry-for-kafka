package com.azure.schemaregistry.samples;

public class CustomerInvoice {
  private String invoiceId;
  private String merchantId;
  private int transactionValueUsd;
  private String userId;

  public CustomerInvoice() {}

  public CustomerInvoice(String invoiceId, String merchantId, int transactionValueUsd, String userId) {
    this.invoiceId = invoiceId;
    this.merchantId = merchantId;
    this.transactionValueUsd = transactionValueUsd;
    this.userId = userId;
  }

  public String getInvoiceId() {
    return this.invoiceId;
  }

  public void setInvoiceId(String id) {
    this.invoiceId = id;
  }

  public String getMerchantId() {
    return this.merchantId;
  }

  public void setMerchantId(String id) {
    this.merchantId = id;
  }

  public int getTransactionValueUsd() {
    return this.transactionValueUsd;
  }

  public void setTransactionValueUsd(int transactionValueUsd) {
    this.transactionValueUsd = transactionValueUsd;
  }

  public String getUserId() {
    return this.userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }
}
