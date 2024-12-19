-- remove old tables if they exist
file removeDB.sql

CREATE TABLE campaigns
(
    campaign_id INT PRIMARY KEY NOT NULL,
    budget      DECIMAL(10, 2) NOT NULL
);

PARTITION TABLE campaigns on COLUMN campaign_id;

CREATE TABLE campaign_ads_object
(
    campaign_id INT  NOT NULL,
    ad_id INT  NOT NULL,
    primary key (campaign_id,ad_id)
);

PARTITION TABLE campaign_ads_object on COLUMN campaign_id;


-- populated by report_bid if needed...
CREATE TABLE campaign_overspends
(
    campaign_id INT PRIMARY KEY NOT NULL,
    budget      DECIMAL(10, 2) NOT NULL,
    spend       DECIMAL(10, 2) NOT NULL
);

PARTITION TABLE campaign_overspends on COLUMN campaign_id;

-- nobody reads this stream, but the view uses it to count stuff...
CREATE STREAM campaign_bids 
PARTITION ON COLUMN campaign_id
(campaign_id INT not null
,ad_id       INT not null
,click_count INT not null
,spend DECIMAL(10, 2));

CREATE VIEW campaign_spends AS
SELECT campaign_id,
       sum(spend) total_spend,
       count(*) how_many
FROM campaign_bids
GROUP BY campaign_id;

CREATE PROCEDURE report_bids 
PARTITION ON TABLE campaign_bids COLUMN campaign_id
AS
BEGIN
--
-- Report spending
--
INSERT INTO campaign_bids (campaign_id, ad_id, click_count, spend) VALUES
    (?,?,?,?);
--
-- add row to overspends if justified
--
UPSERT INTO campaign_overspends
(campaign_id, budget,spend)
SELECT c.campaign_id, c.budget, cs.total_spend 
FROM campaign_spends cs
   , campaigns c
WHERE c.campaign_id = ? 
AND   c.campaign_id = cs.campaign_id
AND   cs.total_spend > c.budget
ORDER BY c.campaign_id, c.budget;
--
END;


CREATE PROCEDURE delete_campaigns 
PARTITION ON TABLE campaign_bids COLUMN campaign_id
AS
BEGIN
--
DELETE FROM campaigns WHERE campaign_id = ?;
--
DELETE FROM campaign_ads_object WHERE campaign_id = ?;
--
DELETE FROM campaign_overspends WHERE campaign_id = ?;
--
END;


 
