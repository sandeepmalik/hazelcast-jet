/*
 *
 *  * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.hazelcast.jet.connector.hbase;

import com.hazelcast.jet.connector.hbase.mapping.Column;
import com.hazelcast.jet.connector.hbase.mapping.Id;

import java.util.UUID;

import static java.lang.Integer.MAX_VALUE;

public class SellerData {

    @Id(name = "SID", ordinal = 0)
    private int sellerId;

    private UUID offerId;

    @Column(family = "0", qualifier = "SKU")
    private String sku;

    @Column(family = "0", qualifier = "PN")
    private String productName;

    @Column(family = "0", qualifier = "WPID")
    private String wpid;

    @Column(family = "0", qualifier = "CAT")
    private String category;

    public int getSellerId() {
        return sellerId;
    }

    public void setSellerId(int sellerId) {
        this.sellerId = sellerId;
    }

    public byte[] getOfferId() {
        return offerId.toString().getBytes();
    }

    @Id(name = "OID", ordinal = 1, length = MAX_VALUE)
    public void setOfferId(byte[] offerId) {
        this.offerId = UUID.nameUUIDFromBytes(offerId);
    }

    public UUID getOfferUUId() {
        return offerId;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getWpid() {
        return wpid;
    }

    public void setWpid(String wpid) {
        this.wpid = wpid;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public String toString() {
        return "SellerData{" +
                "sellerId=" + sellerId +
                ", offerId=" + offerId +
                ", sku='" + sku + '\'' +
                ", productName='" + productName + '\'' +
                ", wpid='" + wpid + '\'' +
                ", category='" + category + '\'' +
                '}';
    }
}
