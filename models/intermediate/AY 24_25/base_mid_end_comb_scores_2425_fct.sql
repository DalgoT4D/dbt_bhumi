select
    d.student_id,
    
    -- Reading Comprehension (RC) Metrics
    -- Baseline
    b.RC_level_base,
    b.RC_grade_level_base,
    b.RC_status_base,
    b.RC_assessed_perc_base,
    -- Midline
    m.RC_level_mid,
    m.RC_grade_level_mid,
    m.RC_status_mid,
    m.RC_assessed_perc_mid,
    -- Endline
    e.RC_level_end,
    e.RC_grade_level_end,
    e.RC_status_end,
    e.RC_assessed_perc_end,
    
    -- Reading Fundamentals (RF) Metrics
    -- Baseline
    b.RF_status_base,
    b.RF_perc_base,
    b.RF_code_base,
    -- Midline
    m.RF_status_mid,
    m.RF_perc_mid,
    m.RF_code_mid,
    -- Endline
    e.RF_status_end,
    e.RF_perc_end,
    e.RF_code_end,
    
    -- Mathematics Core Metrics
    -- Baseline
    b.math_level_base,
    b.math_status_base,
    b.math_mastery_base,
    -- Midline
    m.math_level_mid,
    m.math_status_mid,
    m.math_mastery_mid,
    -- Endline
    e.math_level_end,
    e.math_status_end,
    e.math_mastery_end,
    
    -- Mathematics Subject-wise Scores
    -- Baseline
    b.math_perc_numbers_base,
    b.math_perc_patterns_base,
    b.math_perc_geometry_base,
    b.math_perc_mensuration_base,
    b.math_perc_time_base,
    b.math_perc_operations_base,
    b.math_perc_data_handling_base,
    -- Midline
    m.math_perc_numbers_mid,
    m.math_perc_patterns_mid,
    m.math_perc_geometry_mid,
    m.math_perc_mensuration_mid,
    m.math_perc_time_mid,
    m.math_perc_operations_mid,
    m.math_perc_data_handling_mid,
    -- Endline
    e.math_perc_numbers_end,
    e.math_perc_patterns_end,
    e.math_perc_geometry_end,
    e.math_perc_mensuration_end,
    e.math_perc_time_end,
    e.math_perc_operations_end,
    e.math_perc_data_handling_end,
    
    -- Reading Comprehension Components
    -- Baseline
    b.factual_base,
    b.inference_base,
    b.critical_thinking_base,
    -- Midline
    m.factual_mid,
    m.inference_mid,
    m.critical_thinking_mid,
    -- Endline
    e.factual_end,
    e.inference_end,
    e.critical_thinking_end,
    
    -- Language Skills
    -- Baseline
    b.vocabulary_base,
    b.grammar_base,
    -- Midline
    m.vocabulary_mid,
    m.grammar_mid,
    -- Endline
    e.vocabulary_end,
    e.grammar_end,
    
    -- RF Skills
    -- Baseline
    b.letter_sounds_base,
    b.CVC_words_base,
    b.blends_base,
    b.consonant_diagraph_base,
    b.magic_E_words_base,
    b.vowel_diagraphs_base,
    b.multi_syllabelle_words_base,
    b.passage_1_base,
    b.passage_2_base,
    -- Midline
    m.letter_sounds_mid,
    m.CVC_words_mid,
    m.blends_mid,
    m.consonant_diagraph_mid,
    m.magic_E_words_mid,
    m.vowel_diagraphs_mid,
    m.multi_syllabelle_words_mid,
    m.passage_1_mid,
    m.passage_2_mid,
    -- Endline
    e.letter_sounds_end,
    e.CVC_words_end,
    e.blends_end,
    e.consonant_diagraph_end,
    e.magic_E_words_end,
    e.vowel_diagraphs_end,
    e.multi_syllabelle_words_end,
    e.passage_1_end,
    e.passage_2_end,
    
    -- RF Proficiency Level Distribution
    -- Baseline
    b.developing_base,
    b.beginner_base,
    b.intermediate_base,
    b.advanced_base,
    -- Midline
    m.developing_mid,
    m.beginner_mid,
    m.intermediate_mid,
    m.advanced_mid,
    -- Endline
    e.developing_end,
    e.beginner_end,
    e.intermediate_end,
    e.advanced_end
from {{ref('base_mid_end_comb_students_2425_dim')}} d
left join {{ref('baseline_2425_stg')}} b 
    on d.student_id = b.student_id_base
left join {{ref('midline_2425_stg')}} m 
    on d.student_id = m.student_id_mid
left join {{ref('endline_2425_stg')}} e 
    on d.student_id = e.student_id_end
