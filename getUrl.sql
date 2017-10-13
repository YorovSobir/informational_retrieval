CREATE OR REPLACE FUNCTION get_url(n INT) RETURNS text[]
  AS $get_url$
  DECLARE
    u text;
    result text[];
  BEGIN
    LOCK urls IN SHARE ROW EXCLUSIVE MODE;
    FOR u IN (SELECT url FROM urls LIMIT n)
    LOOP
      DELETE FROM urls WHERE url=u;
      INSERT INTO old_urls VALUES (u);
      result = array_append(result, u);
    END LOOP;
    RETURN result;
  END;
  $get_url$ language plpgsql;
