# Mandi pairs for the owner to decide (not in mandi_aliases.json)

Each row looks like one market under two ids, but one check failed, so it is not joined. Evidence from the R2 archive
(facts/daily, read only, 15 Nov 2026). Add a pair to `farmuncle_pipeline/ingestion/mandi_aliases.json` (aliases and evidence) to join it.

Why held back: names not related (city renames not in `district_aliases`, e.g. Allahabad/Prayagraj, Yeotmal/Yavatmal),
an APMC beside its Uzhavar Sandhai (a different farmers' market), Market I vs II, or similar names with no shared days to compare.

| old id | current id | old name / current name | old range (rows) | current from | shared crop-days, same price |
|---|---|---|---|---|---|
| `up.prayagraj.allahabad` | `up.prayagraj.prayagraj` | Allahabad APMC / Prayagraj APMC | 2018-11-20..2026-01-06 (39,520) | 2025-12-01 | 351, 351 |
| `hp.una.una` | `hp.una.pmy_una` | Una / PMY Una | 2018-11-20..2025-11-04 (36,784) | 2025-11-01 | 89, 89 |
| `hp.hamirpur.hamirpur_nadaun` | `hp.hamirpur.smy_nadaun` | Hamirpur(Nadaun) / SMY Nadaun | 2018-11-20..2025-11-04 (32,628) | 2025-11-01 | 79, 79 |
| `hp.kangra.kangra_baijnath` | `hp.kangra.smy_baijnath` | Kangra(Baijnath) / SMY Baijnath | 2018-11-20..2025-11-04 (28,888) | 2025-11-01 | 52, 52 |
| `wb.howrah.uluberia` | `wb.howrah.nimdhighi` | Uluberia APMC / Nimdhighi Market | 2018-11-20..2026-09-08 (25,854) | 2026-08-02 | 341, 341 |
| `hp.shimla.shimla_and_kinnaur_rampur` | `hp.shimla.smy_rampur` | Shimla and Kinnaur(Rampur) / SMY Rampur | 2019-04-23..2025-11-03 (13,921) | 2025-11-01 | 33, 32 |
| `tr.dhalai.gandacharra` | `tr.dhalai.gondatwisa` | Gandacharra APMC / Gondatwisa APMC | 2018-12-13..2026-05-26 (12,673) | 2026-04-02 | 510, 510 |
| `tn.theni.theni` | `tn.theni.theni_uzhavar` | Theni APMC / Theni(Uzhavar Sandhai ) | 2018-11-20..2026-07-30 (9,049) | 2024-07-16 | 52, 52 |
| `up.bareilly.bahedi` | `up.bareilly.baheri` | Bahedi / Baheri APMC | 2018-11-20..2025-10-13 (6,132) | 2025-12-09 | 0, 0 |
| `tn.cuddalore.panruti` | `tn.cuddalore.panruti_uzhavar` | Panruti APMC / Panruti(Uzhavar Sandhai ) | 2018-11-20..2026-09-08 (6,130) | 2024-07-16 | 24, 24 |
| `mh.yavatmal.yeotmal` | `mh.yavatmal.yavatmal` | Yeotmal APMC / Yavatmal APMC | 2018-11-20..2026-05-13 (6,115) | 2026-04-06 | 81, 81 |
| `tn.theni.cumbum` | `tn.theni.kambam_uzhavar` | Cumbum APMC / Kambam(Uzhavar Sandhai ) | 2018-11-20..2026-05-12 (5,512) | 2024-07-16 | 40, 40 |
| `tn.villupuram.gingee` | `tn.villupuram.gingee_uzhavar` | Gingee APMC / Gingee(Uzhavar Sandhai ) | 2018-11-20..2026-09-08 (5,071) | 2024-07-16 | 34, 34 |
| `mh.mumbai.vashi_new_mumbai` | `mh.mumbai.mumbai_onion_potato` | Vashi New Mumbai APMC / Mumbai-Onion & Potato Market APMC | 2018-11-20..2026-05-13 (4,043) | 2026-04-01 | 68, 68 |
| `tn.salem.attur` | `tn.salem.athur_uzhavar` | Attur APMC / Athur(Uzhavar Sandhai ) | 2018-11-20..2026-06-11 (3,249) | 2024-07-16 | 53, 53 |
| `ka.shivamogga.shimoga_theertahalli` | `ka.shivamogga.thirthahalli` | Shimoga(Theertahalli) APMC / APMC THIRTHAHALLI | 2018-11-25..2026-07-07 (3,007) | 2026-06-01 | 115, 115 |
| `tn.madurai.melur` | `tn.madurai.melur_uzhavar` | Melur APMC / Melur(Uzhavar Sandhai ) | 2018-11-20..2026-07-14 (2,663) | 2024-07-16 | 37, 37 |
| `jk.ganderbal.zazna_ganderbal` | `jk.ganderbal.administrative_zazna` | Zazna Ganderbal (F&V) / Market Administrative Committee Zazna | 2019-06-13..2026-07-16 (2,509) | 2026-06-01 | 57, 57 |
| `tg.karimnagar.manakodur` | `tg.karimnagar.v_saidapur` | Manakodur APMC / V.Saidapur APMC | 2018-12-21..2026-07-01 (2,195) | 2025-11-05 | 49, 49 |
| `tn.ariyalur.jayankondam_rm` | `tn.ariyalur.jeyankondam_uzhavar` | Jayankondam  RM / Jeyankondam (Uzhavar Sandhai ) | 2018-11-26..2026-06-25 (1,941) | 2024-07-16 | 21, 21 |
| `tn.krishnagiri.hosur` | `tn.krishnagiri.hosur_uzhavar` | Hosur / Hosur(Uzhavar Sandhai ) | 2018-11-20..2025-07-10 (1,931) | 2024-07-16 | 68, 68 |
| `tn.thiruvarur.mannargudi` | `tn.thiruvarur.mannargudi_ii_uzhavar` | Mannargudi APMC / Mannargudi II(Uzhavar Sandhai ) | 2018-11-20..2025-12-16 (1,830) | 2024-07-16 | 44, 42 |
| `tn.thiruvarur.thiruvarur` | `tn.tiruvarur.tiruvarur_uzhavar` | Thiruvarur APMC / Tiruvarur(Uzhavar Sandhai ) | 2018-11-20..2026-01-14 (1,786) | 2024-07-16 | 40, 40 |
| `tn.dindigul.palani` | `tn.dindigul.palani_uzhavar` | Palani APMC / Palani(Uzhavar Sandhai ) | 2018-11-20..2026-09-02 (1,768) | 2024-07-16 | 49, 49 |
| `up.siddharth_nagar.wansi` | `up.siddharth_nagar.bansi` | Wansi / Bansi APMC | 2018-11-20..2025-10-24 (1,643) | 2025-11-30 | 0, 0 |
| `tn.dharmapuri.palakode` | `tn.dharmapuri.palacode_uzhavar` | Palakode / Palacode(Uzhavar Sandhai ) | 2018-11-20..2025-07-08 (1,592) | 2024-07-16 | 46, 46 |
| `tn.thoothukudi.thoothukudi_rm` | `tn.thoothukudi.tuticorin_uzhavar` | Thoothukudi RM / Tuticorin(Uzhavar Sandhai ) | 2024-06-18..2026-09-02 (1,552) | 2024-07-16 | 50, 50 |
| `tn.erode.sathyamangalam` | `tn.erode.sathiyamagalam_uzhavar` | Sathyamangalam APMC / Sathiyamagalam(Uzhavar Sandhai ) | 2018-11-22..2026-06-23 (1,378) | 2024-07-16 | 39, 39 |
| `tn.thiruchirappalli.lalgudi_rm` | `tn.thiruchirappalli.lalgudi_uzhavar` | Lalgudi RM / Lalgudi(Uzhavar Sandhai ) | 2019-02-26..2026-05-21 (1,371) | 2024-07-16 | 47, 47 |
| `tn.kancheepuram.kanchipuram` | `tn.kancheepuram.kancheepuram_uzhavar` | Kanchipuram APMC / Kancheepuram(Uzhavar Sandhai ) | 2018-11-20..2026-09-08 (1,285) | 2024-07-16 | 17, 17 |
| `tn.virudhunagar.sathur` | `tn.virudhunagar.sathur_uzhavar` | Sathur APMC / Sathur(Uzhavar Sandhai ) | 2018-11-20..2026-08-25 (1,194) | 2024-07-16 | 30, 30 |
| `tg.karimnagar.ibrahimpatnam` | `tg.karimnagar.v_saidapur` | Ibrahimpatnam APMC / V.Saidapur APMC | 2018-12-21..2026-06-09 (1,143) | 2025-11-05 | 18, 18 |
| `tn.thiruvannamalai.chengam` | `tn.thiruvannamalai.chengam_uzhavar` | Chengam / Chengam(Uzhavar Sandhai ) | 2024-06-18..2025-07-01 (1,127) | 2024-07-16 | 42, 42 |
| `tn.karur.karur` | `tn.karur.velayuthampalayam_uzhavar` | Karur APMC / Velayuthampalayam(Uzhavar Sandhai ) | 2024-06-18..2026-03-27 (1,098) | 2024-07-16 | 30, 30 |
| `tn.karur.karur` | `tn.karur.karur_uzhavar` | Karur APMC / Karur(Uzhavar Sandhai ) | 2024-06-18..2026-03-27 (1,098) | 2024-07-16 | 38, 38 |
| `tn.ramanathapuram.ramanathapuram_phase_3` | `tn.ramanathapuram.ramanathapuram_uzhavar` | Ramanathapuram(phase 3) APMC / Ramanathapuram(Uzhavar Sandhai ) | 2018-11-26..2026-01-21 (958) | 2024-07-16 | 32, 32 |
| `tn.salem.edappadi` | `tn.salem.edapadi_uzhavar` | Edappadi APMC / Edapadi (Uzhavar Sandhai ) | 2024-06-18..2026-02-24 (907) | 2024-07-16 | 32, 32 |
| `tn.dharmapuri.arur` | `tn.dharmapuri.harur_uzhavar` | Arur APMC / Harur(Uzhavar Sandhai ) | 2018-11-20..2025-07-10 (890) | 2024-07-16 | 36, 36 |
| `tg.karimnagar.sultanabad` | `tg.karimnagar.v_saidapur` | Sultanabad APMC / V.Saidapur APMC | 2019-04-22..2026-05-23 (854) | 2025-11-05 | 15, 15 |
| `tg.adilabad.ichoda` | `tg.adilabad.echoda` | Ichoda / Echoda APMC | 2019-02-21..2024-08-06 (694) | 2026-04-24 | 0, 0 |
| `hp.shimla.khadapather` | `hp.shimla.smy_kharapather` | Khadapather / SMY Kharapather | 2024-09-10..2024-10-29 (42) | 2026-08-27 | 0, 0 |
