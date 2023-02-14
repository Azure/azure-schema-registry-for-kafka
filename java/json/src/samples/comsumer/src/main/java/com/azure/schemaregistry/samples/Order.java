package com.azure.schemaregistry.samples;

public class Order {
  private String id;
  private double amount;
  private String description;

  public Order() {}

  public Order(String id, double amount, String description) {
    this.id = id;
    this.amount = amount;
    this.description = description;
  }

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public double getAmount() {
    return this.amount;
  }

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return String.format(
      "{Order Id: %1$s, Order Amount: %2$s, Order Description: %3$s}",
      this.id,
      this.amount,
      this.description);
  }
}
