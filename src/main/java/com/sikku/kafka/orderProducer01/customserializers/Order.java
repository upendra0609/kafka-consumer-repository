package com.sikku.kafka.orderProducer01.customserializers;

public class Order {
	private String customerName;
	private String product;
	private int quantity;

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	@Override
	public String toString() {
		return "Order [customerName=" + customerName + ", product=" + product + ", quantity=" + quantity + "]";
	}
	
	

}
