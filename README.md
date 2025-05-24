# transactions-e-commerce

Nettoyer, transformer et analyser un jeu de données de transactions e-commerce, puis exporter les résultats dans plusieurs formats.

## Source de données 

Le dataset est un ensemble de données transactionnelles qui contient toutes les transactions survenues entre le 01/12/2010 et le 09/12/2011 pour un commerce de détail en ligne non enregistré et basé au Royaume-Uni.

**Source :** https://archive.ics.uci.edu/dataset/352/online+retail  
**Dataset Characteristics :** Multivariate, Sequential, Time-Series  
**Subject Area :** Business  
**Associated Tasks :** Classification, Clustering  
**Feature Type :** Integer, Real  
**Instances :** 541909  
**Features :** 6

### Variables

| Variable Name | Role | Type | Description | Units | Missing Values |
|---------------|------|------|-------------|-------|----------------|
| InvoiceNo	| ID | Categorical | a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation | | no
| StockCode | ID | Categorical | a 5-digit integral number uniquely assigned to each distinct product | | no
| Description |	Feature | Categorical |	product name | | no
| Quantity | Feature | Integer | the quantities of each product (item) per transaction | | no
| InvoiceDate |	Feature | Date | the day and time when each transaction was generated | | no
| UnitPrice | Feature | Continuous | product price per unit	| sterling | no
| CustomerID | Feature | Categorical | a 5-digit integral number uniquely assigned to each customer | | no
| Country |	Feature | Categorical | the name of the country where each customer resides | | no