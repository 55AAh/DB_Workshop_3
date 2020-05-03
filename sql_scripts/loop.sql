DECLARE
    test_chunk_size INT NOT NULL DEFAULT 100;
    test_states_count INT;
BEGIN
    test_chunk_size := &test_chunk_size;
    
    DELETE FROM ZipCode;
    DELETE FROM StateInfo;
    
    test_states_count := TRUNC(SQRT(test_chunk_size));
    
    FOR i in 1..test_states_count LOOP
        INSERT INTO StateInfo(state_id, name) VALUES(i-1, CONCAT('State',i));
    END LOOP;
    
    FOR i IN 1..test_chunk_size LOOP
        INSERT INTO ZipCode(zipcode, StateInfo_state_id) VALUES(i*100, MOD(i, test_states_count));
    END LOOP;
END;