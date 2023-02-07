\c postgres

SELECT to_link, COUNT(*) as count_links 
FROM links
GROUP BY to_link 
ORDER BY count_links DESC 
LIMIT 5;

SELECT from_link, COUNT(*) as count_links 
FROM links
GROUP BY from_link 
ORDER BY count_links DESC 
LIMIT 5;

SELECT links.from_link, AVG(count_children) as mean_children
FROM (SELECT from_link, COUNT(*) as count_children FROM links GROUP BY from_link) as counted INNER JOIN links
ON counted.from_link=links.to_link
GROUP BY links.from_link
HAVING links.from_link='Гідрофобна_взаємодія';

SELECT from_link, to_link FROM links WHERE from_link='Аденозинтрифосфат';
