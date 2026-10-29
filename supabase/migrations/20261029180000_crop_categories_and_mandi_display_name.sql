-- Categories for the crops that had none (ids 202, 314-367), by crop name; unsure ones are 'Other'.
-- 'Fibres' is folded into 'Fiber'. mandis.display_name holds the clean market name for the app
-- ("Bariwala", not "Bariwala APMC"); it is copied from the app DB by scripts/copy_display_names.py.
update crops set category = v.category
from (values
    (202, 'Spices'), (314, 'Fruits'), (315, 'Other'), (316, 'Fruits'), (330, 'Vegetables'), (331, 'Vegetables'),
    (332, 'Fruits'), (333, 'Oilseeds'), (334, 'Vegetables'), (335, 'Vegetables'), (336, 'Herbs & Medicinal'),
    (337, 'Herbs & Medicinal'), (338, 'Oilseeds'), (339, 'Fruits'), (340, 'Other'), (341, 'Nuts & Dry Fruits'),
    (342, 'Other'), (343, 'Livestock'), (344, 'Fiber'), (345, 'Fiber'), (346, 'Other'), (347, 'Fruits'),
    (348, 'Herbs & Medicinal'), (349, 'Millets'), (350, 'Herbs & Medicinal'), (351, 'Millets'),
    (352, 'Herbs & Medicinal'), (353, 'Flowers'), (354, 'Spices'), (355, 'Vegetables'), (356, 'Millets'),
    (357, 'Millets'), (358, 'Other'), (359, 'Pulses'), (360, 'Herbs & Medicinal'), (361, 'Cereals'),
    (362, 'Oilseeds'), (363, 'Other'), (364, 'Livestock'), (365, 'Herbs & Medicinal'),
    (366, 'Herbs & Medicinal'), (367, 'Herbs & Medicinal')
) as v(id, category)
where crops.id = v.id and crops.category is null;

update crops set category = 'Fiber' where category = 'Fibres';

alter table mandis add column if not exists display_name text;
