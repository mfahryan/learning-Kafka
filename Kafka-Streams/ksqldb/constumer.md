### 1. Kita membuat stream dengan schema seperti dibawah
   
```
CREATE STREAM CUSTOMERS (ID BIGINT KEY, FULL_NAME VARCHAR, ADDRESS STRUCT<STREET VARCHAR, CITY VARCHAR, COUNTY_OR_STATE VARCHAR, ZIP_OR_POSTCODE VARCHAR>)
                  WITH (KAFKA_TOPIC='day3-customers',
                        VALUE_FORMAT='AVRO',
                        REPLICAS=1,
                        PARTITIONS=4);
```

### 2. Memasukkan message ke customers

```
INSERT INTO CUSTOMERS VALUES(1,'Opossum, american virginia',STRUCT(STREET:='20 Acker Terrace', CITY:='Lynchburg', COUNTY_OR_STATE:='Virginia', ZIP_OR_POSTCODE:='24515'));
INSERT INTO CUSTOMERS VALUES(2,'Skua, long-tailed',STRUCT(STREET:='7 Laurel Terrace', CITY:='Manassas', COUNTY_OR_STATE:='Virginia', ZIP_OR_POSTCODE:='22111'));
INSERT INTO CUSTOMERS VALUES(3,'Red deer',STRUCT(STREET:='53 Basil Terrace', CITY:='Lexington', COUNTY_OR_STATE:='Kentucky', ZIP_OR_POSTCODE:='40515'));
INSERT INTO CUSTOMERS VALUES(4,'Vervet monkey',STRUCT(STREET:='7615 Brown Park', CITY:='Chicago', COUNTY_OR_STATE:='Illinois', ZIP_OR_POSTCODE:='60681'));
INSERT INTO CUSTOMERS VALUES(5,'White spoonbill',STRUCT(STREET:='7 Fulton Parkway', CITY:='Asheville', COUNTY_OR_STATE:='North Carolina', ZIP_OR_POSTCODE:='28805'));
INSERT INTO CUSTOMERS VALUES(6,'Laughing kookaburra',STRUCT(STREET:='84 Monument Alley', CITY:='San Jose', COUNTY_OR_STATE:='California', ZIP_OR_POSTCODE:='95113'));
INSERT INTO CUSTOMERS VALUES(7,'Fox, bat-eared',STRUCT(STREET:='2946 Daystar Drive', CITY:='Jamaica', COUNTY_OR_STATE:='New York', ZIP_OR_POSTCODE:='11431'));
INSERT INTO CUSTOMERS VALUES(8,'Sun gazer',STRUCT(STREET:='61 Lakewood Gardens Parkway', CITY:='Pensacola', COUNTY_OR_STATE:='Florida', ZIP_OR_POSTCODE:='32590'));
INSERT INTO CUSTOMERS VALUES(9,'American bighorn sheep',STRUCT(STREET:='326 Sauthoff Crossing', CITY:='San Antonio', COUNTY_OR_STATE:='Texas', ZIP_OR_POSTCODE:='78296'));
INSERT INTO CUSTOMERS VALUES(10,'Greater rhea',STRUCT(STREET:='97 Morning Way', CITY:='Charleston', COUNTY_OR_STATE:='West Virginia', ZIP_OR_POSTCODE:='25331'));

```

### 3. Membuat Stream Flatten lalu membuat topic kafka nya

```
CREATE STREAM filtered_customers_all
  WITH (KAFKA_TOPIC='filtered_customers_all_topic',
        VALUE_FORMAT='AVRO')
  AS
  SELECT ID,
         FULL_NAME,
         ADDRESS->STREET AS STREET,
         ADDRESS->CITY AS CITY,
         ADDRESS->COUNTY_OR_STATE AS COUNTY_OR_STATE,
         ADDRESS->ZIP_OR_POSTCODE AS ZIP_OR_POSTCODE
  FROM CUSTOMERS;

```


```
