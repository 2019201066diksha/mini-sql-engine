#!/usr/bin/env python
# coding: utf-8

# In[55]:


import csv
import sqlparse
import sys


# In[56]:


def Pre_Process_Table_Columns(meta_data_file_path='metadata.txt'):
    metadata = open(meta_data_file_path, 'r').readlines()
    metadata = [x.strip() for x in metadata]
    table_with_columns={}
    take_data=False
    take_table_name=False
    column_names=[]
    for x in metadata:
        if x=='<begin_table>':
            take_table_name=True
            continue
        elif x=='<end_table>':
            table_with_columns[table_name]=column_names
            take_data=False
            column_names=[]
            continue
        else:
            if take_table_name==True:
                table_name=x
                take_table_name=False
                continue
            else:
                column_names.append(x)
    return table_with_columns

DICTIONARY = Pre_Process_Table_Columns()


# In[57]:


def break_into_tokens(query):
    s = sqlparse.parse(query)
    l = sqlparse.sql.IdentifierList(s[0].tokens).get_identifiers()
    identifiers=[]
    for x in l:
        identifiers.append(str(x))
    return identifiers


# In[58]:


def find_pattern_number(tokens):
    if len(token_list)==4:
        if token_list[1]=='*':
            return 1
        elif token_list[1][:4] == 'max(' or token_list[1][:4] == 'min(' or token_list[1][:4] == 'sum(' or token_list[1][:8] == 'average(':
            return 2
        else:
            return 3
    elif len(token_list)==5:
        return 4
    else:
        return 5


# In[59]:


def get_all_table_names(tables_name):
    table_names = list(tables_name.split(','))
    table_names = [ strng.rstrip().lstrip() for strng in table_names]
    return table_names


# In[60]:


def get_csv_filenames_of_table_names(table_names):
    csv_names = [x+'.csv' for x in table_names]
    return csv_names


# In[115]:


def read_from_name(table_name):
    encoded_data = list(csv.reader(open(table_name)))
    data = []
    for i in range(len(encoded_data)):
        data.append([])
        for j in range(len(encoded_data[i])):
            if encoded_data[i][j][0] == '“':
                data[i].append(encoded_data[i][j][1:-1])
            else:
                data[i].append(encoded_data[i][j])
    return data


# In[116]:


def merge_two_tables(table1,table2):
    merged_table=[]
    merged_table.append(table1[0]+table2[0])
    merged_table.append(table1[1]+table2[1])
    for r1 in range(2,len(table1)):
        for r2 in range(2,len(table2)):
            merged_table.append(table1[r1]+table2[r2])
    return merged_table


# In[117]:


def merge_tables(table_names,DICTIONARY):
    

    csv_file_name = get_csv_filenames_of_table_names(table_names)
    first_table = []
    col_name = DICTIONARY[table_names[0]]
    header_col_name = [table_names[0]+'.'+x for x in col_name]
    first_table.append(header_col_name)
    first_table.append(col_name)
    first_table = first_table + read_from_name(csv_file_name[0]) #list(csv.reader(open(csv_file_name[0])))
    second_table=[]
    for i in range(1,len(csv_file_name)):
        col_name = DICTIONARY[table_names[i]]
        header_col_name = [table_names[i]+'.'+x for x in col_name]
        second_table.append(header_col_name)
        second_table.append(col_name)
        second_table = second_table + read_from_name(csv_file_name[i])#list(csv.reader(open(csv_file_name[i])))

        first_table = merge_two_tables(first_table,second_table)
    return first_table


# In[ ]:





# In[ ]:





# In[118]:


def drop_column(table,column_number):
    for row in table:
        del row[column_number]
    return table


# In[119]:


def trim_table(table,columns_list):
    table_table_col_name=table[0]
    table_col_name=table[1]
    i=0
    while( i < len(table_table_col_name)):
        if table_table_col_name[i] in columns_list or table_col_name[i] in columns_list:
            pass
        else:
            table = drop_column(table,i)
            i=i-1
        i=i+1
    return table
        


# In[120]:


def find_column_index_for_column_name(table,column_name):
    #handle error if there are multiple columns with same name and no column with name
    table_table_col_name=table[0]
    table_col_name=table[1]
    for i in range(len(table_col_name)):
        if table_table_col_name[i] ==column_name or table_col_name[i] ==column_name:
            return i


# In[121]:


def operation_between_two_elements(op,e1,e2): 
    switcher = { 
        '=': e1==e2, 
        '<': e1<e2, 
        '>': e1>e2,
        '>=': e1>=e2,
        '<=': e1<=e2
    } 
  
    return switcher.get(op)


# In[122]:


def comparision_binary_operation(table,operation,column1_name,column2_name):
    first_index = find_column_index_for_column_name(table,column1_name)
    second_index = find_column_index_for_column_name(table,column2_name)

    i=2
    result_mat=[]
    while(i<len(table)):
        if operation_between_two_elements(operation,int(table[i][first_index]),int(table[i][second_index])):
            result_mat.append(True)
        else:
            result_mat.append(False)
        i=i+1
    return result_mat
    


# In[123]:


def comparision_binary_operation_with_num(table,operation,column1_name,num):
    try:
        num1 = int(column1_name)
        result_mat=[]
        i=2
        while(i<len(table)):
            if operation_between_two_elements(operation,num1,num):
                result_mat.append(True)
            else:
                result_mat.append(False)
            i=i+1
        return result_mat
    except:

        first_index = find_column_index_for_column_name(table,column1_name)
        i=2
        result_mat=[]
        while(i<len(table)):
            if operation_between_two_elements(operation,int(table[i][first_index]),num):
                result_mat.append(True)
            else:
                result_mat.append(False)
            i=i+1
        return result_mat


# In[124]:


def result_after_binary_operation_and_or(table,yes_no_remove,operation1,column1_name,column2_name,operation2=None,column3_name=None,column4_name=None,and_or=None):
    try:
        number_ = int(column1_name)
        temp = column1_name
        column1_name=column2_name
        column2_name = temp
        if(operation1==">="):
            operation1="<="
        elif(operation1=="<="):
            operation1=">="
        elif(operation1==">"):
            operation1="<"
        elif(operation1=="<"):
            operation1=">"
    except:
        pass
    
    try:
        i=2
        sec=False
        try:
            num = int(column2_name)
            sec=True
            result1 = comparision_binary_operation_with_num(table,operation1,column1_name,num)
        except:
            result1 = comparision_binary_operation(table,operation1,column1_name,column2_name)

        if and_or is None:

        ####### For single condition ####################
            k=0
            while(i<len(table)):

                if result1[k]==True:
                    pass
                else:   
                    del table[i]
                    i=i-1

                i=i+1
                k=k+1
            if operation1 == '=' and sec == False and yes_no_remove:
                print("Ss")
                first_index = find_column_index_for_column_name(table,column2_name)
                table = drop_column(table,first_index)
            return table,""
        ########### FOr Multiple #########################

        try:
            number_ = int(column3_name)
            temp = column3_name
            column3_name=column4_name
            column4_name = temp
            if(operation2==">="):
                operation2="<="
            elif(operation2=="<="):
                operation2=">="
            elif(operation2==">"):
                operation2="<"
            elif(operation2=="<"):
                operation2=">"
        except:
            pass

        try:
            num = int(column4_name)

            result2 = comparision_binary_operation_with_num(table,operation2,column3_name,num)
        except:
            result2 = comparision_binary_operation(table,operation2,column3_name,column4_name)

        k=0
        while(i<len(table)):
            if and_or=='or':
                if result1[k]==True or result2[k]==True:
                    pass
                else:   
                    del table[i]
                    i=i-1
            else:
                if result1[k]==True and result2[k]==True:
                    pass
                else:   
                    del table[i]
                    i=i-1
            i=i+1
            k=k+1
        if operation1 == '=' and sec == False and and_or=='and':
            first_index = find_column_index_for_column_name(table,column2_name)
            table = drop_column(table,first_index)
        return table,""
    except:
        return None,"Error in Where part"


# In[125]:


def get_column(table,column_number):
    col=[]
    for row in table:
        col.append(row[column_number])
    return col


# In[126]:


def trim_table2(table,columns_list):
    table_table_col_name=table[0]
    table_col_name=table[1]
    new_table =[]
    for column_name in columns_list:
        ind = find_column_index_for_column_name(table,column_name)
        new_table.append(get_column(table,ind))
    final_table=[]
    for i in range(len(new_table[0])):
        p=[]
        for j in range(len(new_table)):
            p.append(new_table[j][i])
        final_table.append(p)
    return final_table
        


# In[127]:


def aggregate_function(column,func): 

    column = [int(x) for x in column]

    switcher = { 
        'max': max(column), 
        'min': min(column), 
        'average': sum(column)/len(column),
        'sum': sum(column),
    } 
  
    val = switcher.get(func)
    if val is None:
        val = 'No such function exists'
    return val


# In[128]:


def print_row(row):
    string_row  = ""
    i=0
    while(i<len(row)-1):
        string_row=string_row+str(row[i])+","
        i+=1
    print(string_row+str(row[i]))


# In[129]:


def print_table(table):
    print_row(table[0])
    for i in range(2,len(table)):
        print_row(table[i])


# In[130]:


def FROM(token_list):
    i=0
    while(i<len(token_list)):
        if token_list[i].lower() == 'from':
            break;
        i=i+1
    if i == len(token_list) or i == len(token_list)-1:
        return None,0
    return token_list[i+1],i+1


# In[131]:


def get_where_conditions(string):
    op=None
    if " and " in string:
        conditions = list(string.split('and'))
        op='and'
    elif " or " in string:
        conditions = list(string.split('or'))
        op='or'
    else:
        conditions = list(string.split('or'))

    conditions[0] = conditions[0].strip()
    if conditions[0][:5].lower() !="where":
        return None,"Error in place of WHERE"
    conditions[0]=conditions[0][5:]
    conditions = [ strng.strip() for strng in conditions]
    return conditions,op


# In[132]:


def WHERE(token_list,i):
    conditions,op = get_where_conditions(token_list[i])
    return conditions,op


# In[133]:


def find_triplet(string):
    if "<=" in string:
        ls = string.split('<=')
        A=ls[0].strip()
        B=ls[1].strip()
        OP = "<="
    elif ">=" in string:
        ls = string.split('>=')
        A=ls[0].strip()
        B=ls[1].strip()
        OP = ">="
    elif ">" in string:
        ls = string.split('>')
        A=ls[0].strip()
        B=ls[1].strip()
        OP=">"
    elif "<" in string:
        ls = string.split('<')
        A=ls[0].strip()
        B=ls[1].strip()
        OP="<"
    elif "=" in string:
        ls = string.split('=')
        A=ls[0].strip()
        B=ls[1].strip()
        OP="="
    return A,B,OP


# In[134]:


def get_all_col_names(col_name):
    c_names = list(col_name.split(','))
    c_names = [ strng.rstrip().lstrip() for strng in c_names]
    return c_names


# In[135]:


def remove_duplicates(table):
    i=0
    while i < len(table):
        j=i+1
        while j < len(table):
            if table[i]==table[j]:
                del table[j]
                j=j-1
            j=j+1
        i=i+1
    return table


# In[136]:


def execute(query):
#     try:  
        token_list = break_into_tokens(query)
        if token_list[0].lower()!='select':
            return None,"SELECT NOT FOUND"
        FROM_STRING,loc = FROM(token_list)
        if FROM_STRING == None:
            return None,"Error near FROM keyword"
        table_names = get_all_table_names(FROM_STRING)
        FROM_TABLE = merge_tables(table_names,DICTIONARY)
        WHERE_TABLE=FROM_TABLE
        if loc+1 != len(token_list):
            if(token_list[loc+1][:5].lower()!="where"):
                return None,"Error after From ,Where not found"
            WHERE_STRING,op = WHERE(token_list,loc+1)
            if WHERE_STRING==None:
                return None,op
            yes_no_remove=False
            if op is None:
                
                if token_list[1] == '*':
                    yes_no_remove=True
                A,B,O = find_triplet(WHERE_STRING[0])
                WHERE_TABLE,v = result_after_binary_operation_and_or(FROM_TABLE,yes_no_remove,O,A,B)
            else:
                A1,B1,O1 = find_triplet(WHERE_STRING[0])
                A2,B2,O2 = find_triplet(WHERE_STRING[1])
                WHERE_TABLE,v = result_after_binary_operation_and_or(FROM_TABLE,yes_no_remove,O1,A1,B1,O2,A2,B2,op)
            if WHERE_TABLE is None:
                return None,v
        selec_columns=1
        FINAL=WHERE_TABLE
      
        if token_list[1] == 'distinct':
            selec_columns=2
        if "max" in token_list[selec_columns] or "min" in token_list[selec_columns] or "average" in token_list[selec_columns] or "sum" in token_list[selec_columns]:
            part = token_list[selec_columns].strip()

            if part[:3]=="max":
                fun = part[:3]
                colm = part[4:-1]
            elif part[:3] == "min":
                fun = part[:3]
                colm = part[4:-1]
            elif part[:3]=="sum":
                fun = part[:3]
                colm = part[4:-1]
            elif part[:7]=="average":
                fun = part[:7]
                colm = part[8:-1]
            else:
                return None,"Errot in aggregate function,not found"
            return aggregate_function(get_column(FINAL,find_column_index_for_column_name(FINAL,colm))[2:],fun),"A"
        FINAL=WHERE_TABLE
        if token_list[selec_columns] != "*":        
            cols = get_all_col_names(token_list[selec_columns])
            FINAL = trim_table2(WHERE_TABLE,cols)
        if selec_columns==2:
            FINAL = remove_duplicates(FINAL)
        return FINAL,"M"
#     except:
#         return None,"Error,check syntax"


# In[137]:


def sql_query(query):
    res,v=execute(query)
#     print(res)
    if res==None:
        print(v)
        return
    if v=="M":
        print_table(res)
    elif v=="A":
        print(res)
    else:
        print(v)


# In[138]:


def mainer(query):
    query=query.strip()
    if(query[-1]!=';'):
        print("';' not present")
        return
    query=query[:-1]
    sql_query(query)


# In[139]:


mainer(sys.argv[1])
# mainer("select * from table1;")


# In[ ]:




