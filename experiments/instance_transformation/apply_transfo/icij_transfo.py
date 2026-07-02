from run_transfo import *

def r9_1():
    print("=*10")
    print("R9_1")
    print("=*10")

    create_node("T_Officer", "Officer")
    create_node("T_Address", "Address")
    add_prop("T_Address", "source", "Address", "sourceID")
    create_node("T_Country", "Country")
    create_edge_from_edge("T_REG_ADDRESS", "T_Officer", "T_Address", "registered_address", "Officer", "Address")
    add_prop("T_Country", "name", "Country", "name")

def r1_3_1():
    print("=*10")
    print("R1_3_1")
    print("=*10")
    create_node("T_Intermediary", "Intermediary")
    add_prop("T_Intermediary", "source", "Intermediary", "sourceID")
    create_node("T_Address", "Address")
    add_prop("T_Address", "valid_until", "Address", "valid_until")
    create_edge_from_edge("T_REG_ADDRESS", "T_Intermediary", "T_Address", "registered_address", "Intermediary", "Address")


def r10_13_1():
    print("=*10")
    print("R10_3_1")
    print("=*10")
    add_prop("T_Officer", "status", "Officer", "status")
    add_prop("T_Officer", "source", "Officer", "sourceID")
    add_prop("T_Officer", "name", "Officer", "name")

def r10_13_2():
    print("=*10")
    print("R10_3_2")
    print("=*10")
    add_prop("T_Address", "address", "Address", "address")
    add_prop("T_Address", "valid_until", "Address", "valid_until")
    add_prop("T_Address", "orig_addr", "Address", "original_address")

def r14_1():
    print("=*10")
    print("R14_1")
    print("=*10")
    create_edge_from_edge("T_SAME_AS", "T_Address", "T_Address", "same_as", "Address", "Address")

def r10_13_3():
    print("=*10")
    print("R10_13_3")
    print("=*10")

    add_prop("T_Intermediary", "name", "Intermediary", "name")
    add_prop("T_Intermediary", "valid_until", "Intermediary", "valid_until")
    add_prop("T_Intermediary", "status", "Intermediary", "status")

def r14_2():
    print("=*10")
    print("R14_2")
    print("=*10")
    create_edge_from_edge("T_SIMILAR", "T_Officer", "T_Officer", "similar", "Officer", "Officer")


def r10_13_4():
    print("=*10")
    print("R10_13_4")
    print("=*10")

    create_node("T_Entity", "Entity")
    add_prop("T_Entity", "name", "Entity", "name")
    add_prop("T_Entity", "inact_date", "Entity", "inactivation_date")
    add_prop("T_Entity", "orig_name", "Entity", "original_name")
    add_prop("T_Entity", "source", "Entity", "sourceID")
    add_prop("T_Entity", "inc_date", "Entity", "incorporation_date")

def r6_1():
    print("=*10")
    print("R6_1")
    print("=*10")
    create_node_from_prop("T_Jurisdiction", "Entity", "jurisdiction")
    add_prop_from("T_Jurisdiction", "juris", "Entity", "jurisdiction", "jurisdiction")
    create_edge("T_IN_JURIS", "T_Entity", "T_Jurisdiction", "jurisdiction", "juris")
    print(f"Create T_Jurisdiction-[T_RELATED]->T_Country")
    run_query("""
    match (te:T_Entity)-[:T_IN_JURIS]->(j:T_Jurisdiction)
    match (e: Entity {node_id: te.node_id})
    match (c:T_Country {name: e.countries})
    CREATE (j)-[r:T_RELATED]->(c)
    """)

def additional_edge():
    print("=*10")
    print("additional")
    print("=*10")
    create_edge("T_LOCATED", "T_Address", "T_Country", "countries", "name")


r9_1()

# r1_3_1()
#
# r10_13_1()
#
# r10_13_2()
#
# r14_1()
#
# r10_13_3()
#
# r14_2()
#
# r10_13_4()
#
# r6_1()
#
# additional_edge()







