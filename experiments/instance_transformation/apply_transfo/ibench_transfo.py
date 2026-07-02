from run_transfo import *

# ======================
# Merge
# ======================


def merge():
    print("=*10")
    print("Merge")
    print("=*10")

    create_node("vote_me_0_nl0_ce0", "store_me_0_nl0_ce1")

    add_prop(
        "vote_me_0_nl0_ce0",
        "free_me_0_nl1_ae3",
        "store_me_0_nl0_ce1",
        "free_me_0_nl1_ae3",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "scale_me_0_nl1_ae0",
        "store_me_0_nl0_ce1",
        "scale_me_0_nl1_ae0",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "improve_me_0_nl0_ae3",
        "enable_me_0_nl0_ce0",
        "improve_me_0_nl0_ae3",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "wish_me_0_nl0_ae0",
        "enable_me_0_nl0_ce0",
        "wish_me_0_nl0_ae0",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "purpose_me_0_nl1_ae1",
        "store_me_0_nl0_ce1",
        "purpose_me_0_nl1_ae1",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "soap_me_0_nl0_ae0comp1_joinattr_0",
        "store_me_0_nl0_ce1",
        "soap_me_0_nl0_ae0comp1_joinattr_0",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "wait_me_0_nl1_ae2",
        "store_me_0_nl0_ce1",
        "wait_me_0_nl1_ae2",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "page_me_0_nl0_ae2",
        "enable_me_0_nl0_ce0",
        "page_me_0_nl0_ae2",
    )
    add_prop(
        "vote_me_0_nl0_ce0",
        "listen_me_0_nl0_ae1",
        "enable_me_0_nl0_ce0",
        "listen_me_0_nl0_ae1",
    )

    del_node("store_me_0_nl0_ce1")
    del_node("enable_me_0_nl0_ce0")


# ======================
# Split
# ======================


def split():
    print("=*10")
    print("Split")
    print("=*10")

    create_node("send_vp_0_nl0_ce1", "come_vp_0_nl0_ce0")
    create_node("spoon_vp_0_nl0_ce0", "come_vp_0_nl0_ce0")

    add_prop(
        "send_vp_0_nl0_ce1",
        "account_vp_0_nl0_ae3",
        "come_vp_0_nl0_ce0",
        "account_vp_0_nl0_ae3",
    )
    add_prop(
        "send_vp_0_nl0_ce1",
        "self_vp_0_nl0_ae4",
        "come_vp_0_nl0_ce0",
        "self_vp_0_nl0_ae4",
    )
    add_prop(
        "send_vp_0_nl0_ce1",
        "chess_vp_0_nl0_ae2",
        "come_vp_0_nl0_ce0",
        "chess_vp_0_nl0_ae2",
    )
    add_prop(
        "spoon_vp_0_nl0_ce0",
        "break_vp_0_nl0_ae1",
        "come_vp_0_nl0_ce0",
        "break_vp_0_nl0_ae1",
    )
    add_prop(
        "spoon_vp_0_nl0_ce0",
        "sound_vp_0_nl0_ae0ke0",
        "come_vp_0_nl0_ce0",
        "sound_vp_0_nl0_ae0ke0",
    )

    create_edge(
        "card_vp_0_nl0_ae0joinattrref", "send_vp_0_nl0_ce1", "spoon_vp_0_nl0_ce0"
    )

    del_node("come_vp_0_nl0_ce0")


# ======================
# Add props
# ======================


def add_prop_transfo():
    print("=*10")
    print("Add props")
    print("=*10")

    import_prop("fish_ad_0_nl0_ce0", "feeble_ad_0_nl0_ae6", "fish_ad_0_nl0_ce0", 1)
    import_prop("fish_ad_0_nl0_ce0", "produce_ad_0_nl0_ae5", "fish_ad_0_nl0_ce0", 1)


# ======================
# Rem props
# ======================


def rem_prop_transfo():
    print("=*10")
    print("Rem props")
    print("=*10")

    del_prop("earth_dl_0_nl0_ce0", "rod_dl_0_nl0_ae4")


merge()
