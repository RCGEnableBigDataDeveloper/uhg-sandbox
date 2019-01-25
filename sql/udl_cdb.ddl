
CREATE DATABASE UDL_CDB;

USE UDL_CDB;

DROP TABLE cust_info;
CREATE EXTERNAL TABLE IF NOT EXISTS
cust_info
(
KEY VARCHAR(256),
alt_id_asgn_typ_cd VARCHAR(256),
asgn_cov_lvl_typ_ind VARCHAR(256),
cdc_flag VARCHAR(256),
cdc_ts VARCHAR(256),
cust_canc_rsn_typ_cd VARCHAR(256),
cust_id_canc_dt VARCHAR(256),
cust_id_eff_dt VARCHAR(256),
cust_lgl_nm VARCHAR(256),
cust_loc_nbr INT,
cust_nm VARCHAR(256),
cust_tax_id DOUBLE,
diff_fam_mbr_adr_ind VARCHAR(256),
elig_analyst1_nm VARCHAR(256),
elig_analyst1_racf_id VARCHAR(256),
elig_analyst2_nm VARCHAR(256),
elig_analyst2_racf_id VARCHAR(256),
elig_loc_typ_cd VARCHAR(256),
hist_purge_days VARCHAR(256),
intgr_card_typ_cd VARCHAR(256),
lab_typ_cd VARCHAR(256),
lgcy_src_cust_id VARCHAR(256),
mdcr_xovr_cd VARCHAR(256),
mig_src_cd VARCHAR(256),
mkt_seg_typ_cd VARCHAR(256),
ofc_of_sale_cd VARCHAR(256),
optum_dt VARCHAR(256),
org_typ_cd VARCHAR(256),
pseudo_pcp_typ_cd VARCHAR(256),
racf_id VARCHAR(256),
ren_mo_cd VARCHAR(256),
ren_yr_nbr VARCHAR(256),
row_sts_cd VARCHAR(256),
row_tmstmp VARCHAR(256),
row_user_id VARCHAR(256),
rpt_seg_typ_cd VARCHAR(256),
src_cd VARCHAR(256),
src_tmstmp VARCHAR(256),
strct_src_cd VARCHAR(256),
updt_typ_cd VARCHAR(256),
ingfilename VARCHAR(256),
snapshot_trkid VARCHAR(256),
recordmd5 VARCHAR(256),
snapshot_crt_ts VARCHAR(256),
recordtime TIMESTAMP
)
STORED AS PARQUET
location  '/datalake/uhc/ei/pi_ara/data_sources/other_ds/members/cdb_onshore/CUST_INFO/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

DROP TABLE pol_info;
CREATE EXTERNAL TABLE IF NOT EXISTS
pol_info
(
    KEY VARCHAR(256),
    cdc_flag VARCHAR(256),
    cdc_ts VARCHAR(256),
    lgcy_clm_eng_cd VARCHAR(256),
    lgcy_pol_canc_dt VARCHAR(256),
    lgcy_pol_eff_dt VARCHAR(256),
    lgcy_pol_nbr VARCHAR(256),
    lgcy_pol_nm VARCHAR(256),
    lgcy_src_cust_id VARCHAR(256),
    pol_lead_partner_cd VARCHAR(256),
    pol_size_cd VARCHAR(256),
    racf_id VARCHAR(256),
    row_sts_cd VARCHAR(256),
    row_tmstmp VARCHAR(256),
    row_user_id VARCHAR(256),
    src_cd VARCHAR(256),
    src_tmstmp VARCHAR(256),
    updt_typ_cd VARCHAR(256),
    ingfilename VARCHAR(256),
    snapshot_trkid VARCHAR(256),
    recordmd5 VARCHAR(256),
    snapshot_crt_ts VARCHAR(256)
) STORED AS PARQUET
location  '/datalake/uhc/ei/pi_ara/data_sources/other_ds/members/cdb_onshore/POL_INFO/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

DROP TABLE l_cov_prdt_dt;
CREATE EXTERNAL TABLE IF NOT EXISTS
    l_cov_prdt_dt
    (
        KEY VARCHAR(256),
        billing_subgrp_nbr VARCHAR(256),
        billing_sufx_cd VARCHAR(256),
        bil_typ_cd VARCHAR(256),
        cancel_rsn_typ_cd VARCHAR(256),
        cdc_flag VARCHAR(256),
        cdc_ts VARCHAR(256),
        ces_grp_nbr VARCHAR(256),
        clm_sys_typ_cd VARCHAR(256),
        cnsm_id INT,
        cnsm_lgl_enty_nm VARCHAR(256),
        cobra_eff_dt VARCHAR(256),
        cobra_mo VARCHAR(256),
        cobra_qual_evnt_cd VARCHAR(256),
        cos_div_cd VARCHAR(256),
        cos_grp_nbr VARCHAR(256),
        cos_pnl_nbr DOUBLE,
        cov_canc_dt VARCHAR(256),
        cov_eff_dt VARCHAR(256),
        cov_lvl_typ_cd VARCHAR(256),
        cov_pd_thru_dt VARCHAR(256),
        cov_pd_thru_rsn_cd VARCHAR(256),
        cov_typ_cd VARCHAR(256),
        deriv_cov_ind VARCHAR(256),
        ebill_dt VARCHAR(256),
        ee_sts_typ_cd VARCHAR(256),
        elig_grc_prd_thru_dt VARCHAR(256),
        elig_sys_typ_cd VARCHAR(256),
        fund_typ_cd VARCHAR(256),
        govt_pgm_typ_cd VARCHAR(256),
        grndfathered_pol_ind VARCHAR(256),
        h_cntrct_id VARCHAR(256),
        indv_grp_typ_cd VARCHAR(256),
        lgcy_ben_pln_id VARCHAR(256),
        lgcy_pln_var_cd VARCHAR(256),
        lgcy_pol_nbr VARCHAR(256),
        lgcy_prdt_cd VARCHAR(256),
        lgcy_prdt_id VARCHAR(256),
        lgcy_prdt_typ_cd VARCHAR(256),
        lgcy_rpt_cd VARCHAR(256),
        lgcy_src_id VARCHAR(256),
        list_bill_typ_cd VARCHAR(256),
        lst_prem_pd_dt VARCHAR(256),
        medica_trvlben_ind VARCHAR(256),
        mkt_site_cd VARCHAR(256),
        mkt_typ_cd VARCHAR(256),
        partn_nbr SMALLINT,
        pbp_cd VARCHAR(256),
        plan_cd VARCHAR(256),
        pol_ren_dt VARCHAR(256),
        prdt_srvc_typ_cd VARCHAR(256),
        prfl_id INT,
        prr_cov_mo VARCHAR(256),
        racf_id VARCHAR(256),
        rate_cov_typ_cd VARCHAR(256),
        retro_days VARCHAR(256),
        retro_elig_recv_dt VARCHAR(256),
        retro_orig_cov_canc_dt VARCHAR(256),
        retro_orig_cov_eff_dt VARCHAR(256),
        retro_ovrd_typ_cd VARCHAR(256),
        retro_typ_cd VARCHAR(256),
        risk_typ_cd VARCHAR(256),
        row_sts_cd VARCHAR(256),
        row_tmstmp VARCHAR(256),
        row_user_id VARCHAR(256),
        rr_ben_grp_cho_cd VARCHAR(256),
        rr_ben_grp_nbr VARCHAR(256),
        rr_br_cd VARCHAR(256),
        rr_optout_plan_ind VARCHAR(256),
        rr_un_cd VARCHAR(256),
        sec_typ_cd VARCHAR(256),
        shr_arng_cd VARCHAR(256),
        shr_arng_oblig_cd VARCHAR(256),
        src_cd VARCHAR(256),
        src_cdb_xref_id INT,
        src_cov_mnt_typ_cd VARCHAR(256),
        src_tmstmp VARCHAR(256),
        state_of_issue_cd VARCHAR(256),
        tops_cov_lvl_typ_cd VARCHAR(256),
        updt_typ_cd VARCHAR(256),
        xref_id_partn_nbr SMALLINT,
        ingfilename VARCHAR(256),
        snapshot_trkid VARCHAR(256),
        recordmd5 VARCHAR(256),
        snapshot_crt_ts VARCHAR(256)
    ) STORED AS PARQUET
    location  '/datalake/uhc/ei/pi_ara/data_sources/other_ds/members/cdb_onshore/L_COV_PRDT_DT/'
    TBLPROPERTIES ("parquet.compression"="SNAPPY");


CREATE TABLE INFO_FULL AS SELECT A.*, B.MKT_SEG_TYP_CD FROM UDL_CDB.L_COV_PRDT_DT A
LEFT JOIN
(SELECT * FROM UDL_CDB.POL_INFO P, UDL_CDB.CUST_INFO C
WHERE upper(trim(P.ROW_STS_CD)) = 'A'
AND upper(trim(C.ROW_STS_CD)) = 'A'
AND P.LGCY_SRC_CUST_ID = C.LGCY_SRC_CUST_ID
) B
ON A.LGCY_POL_NBR = B.LGCY_POL_NBR;

2809429981
