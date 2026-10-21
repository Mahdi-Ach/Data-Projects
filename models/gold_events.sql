SELECT
    ville,
    AVG(temperature) as temp_moyenne,
    AVG(humidite) as humidite_moyenne,
    MAX(vent) as vent_max,
    COUNT(*) as nb_releves
FROM events_clean
GROUP BY ville