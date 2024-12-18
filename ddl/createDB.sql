-- remove old tables if they exist
file removeDB.sql

CREATE TABLE campaigns
(
    campaign_id INT PRIMARY KEY NOT NULL,
    budget      DECIMAL(10, 2) NOT NULL
);

PARTITION TABLE campaigns on COLUMN campaign_id;

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
INSERT INTO campaign_bids (campaign_id, click_count, spend) VALUES
    (?,?,?);
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
DELETE FROM campaign_overspends WHERE campaign_id = ?;
--
END;


 
    exec report_bids 1  100  15.00 1;
    exec report_bids 1  50  75.00 1;


