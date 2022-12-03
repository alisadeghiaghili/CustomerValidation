import sqlalchemy as sa
import pandas as pd
import re

config = 'mssql+pyodbc://172.16.1.119/Future_DM?driver=SQL+Server+Native+Client+11.0'
engine = sa.create_engine(config)

DimCustomerQuery = 'Select * from [Auction_DM].[Auction_Dim].[Customer]'
DimCustomer = pd.read_sql_query(DimCustomerQuery, engine)
DimCustomer['nameChanged'] = [name.replace(' ', '') for name in DimCustomer.Name]
DimCustomer.sort_values(by = "nameChanged", inplace = True)

ProcedureQuery = '''SELECT [OldCustomerID] code
                          ,[CustomerName] name
                           tionalCode
                    FROM CUSTOMERSERVER.[CustomersManagement].[Credits].[vGetCustoemrs]
                    where LEN([OldCustomerID]) < 8 and [OldCustomerID]  > 0
                    order by name'''
CustomerValidList = pd.read_sql_query(ProcedureQuery, engine)
CustomerValidList['nameChanged'] = [name.replace(' ', '') for name in CustomerValidList.name]
CustomerValidList.sort_values(by = "nameChanged", inplace = True)

# result = pd.merge(left = CustomerValidList, right = DimCustomer, on = "nameChanged", how = "inner").sort_values(by = "nameChanged")

# alterResult = pd.DataFrame(columns=['ID', 'Name', 'CurrentNationalCode', 'OtherNationalCode','NeedsUpdate'])

# for indv, rowv in CustomerValidList.iterrows():
#     i = 0
#     for indc, rowc in DimCustomer.iterrows():
#         print("valid ", indv, "customer", indc)
#         if rowv.nameChanged == rowc.nameChanged:
#             i = 1
#             if rowv.NationalCode != rowc.NationalID:
#                 print(rowv)
#                 alterResult = alterResult.append(pd.Series({'ID': rowc.ID, 'Name': rowc.Name, 'CurrentNationalCode': rowc.NationalID, 'OtherNationalCode': rowv.NationalCode, 'NeedsUpdate': 1}), ignore_index = True)
#         else:
#             if i == 1:
#                 break
                
alterResult = pd.DataFrame(columns=['ID', 'Name', 'CurrentNationalCode', 'OtherNationalCode','NeedsUpdate'])

c = 0
for indv, rowv in CustomerValidList.iterrows():
    c += 1
    i = 0
    res = DimCustomer[DimCustomer.nameChanged == rowv.nameChanged]
    print(c)
    for indr, rowr in res.iterrows():
        # print("valid ", indv, "customer", indr)
        if rowv.nameChanged == rowr.nameChanged:
            i = 1
            if rowv.NationalCode != rowr.NationalID:
                print(rowv)
                alterResult = alterResult.append(pd.Series({'ID': rowr.ID, 'Name': rowr.Name, 'CurrentNationalCode': rowr.NationalID, 'OtherNationalCode': rowv.NationalCode, 'NeedsUpdate': 1}), ignore_index = True)
        else:
            if i == 1:
                break

alterResult.drop_duplicates(inplace = True)
alterResult.to_excel(r"D:\conflicts.xlsx")

DuplicateResult = pd.DataFrame(columns=['Name', 'IDs'])

c = 0
for ind1, row1 in DimCustomer.iterrows():
    IDsList = []
    c += 1
    res = DimCustomer[DimCustomer.nameChanged == row1.nameChanged]
    print(c)
    if res.shape[0] > 1:
        for ind2, row2 in res.iterrows():
            if row1.nameChanged == row2.nameChanged:
                IDsList.append(row2.ID)        
        DuplicateResult = DuplicateResult.append(pd.Series({'Name': row1.Name, 'IDs': IDsList}), ignore_index = True)

DuplicateResult = DuplicateResult.iloc[DuplicateResult.astype(str).drop_duplicates().index]
DuplicateResult.to_excel(r"D:\duplicates.xlsx")
