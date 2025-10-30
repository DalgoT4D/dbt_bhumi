select
    d.student_id,
    
    -- Reading Comprehension (RC) Metrics
    -- Baseline
    b.rc_level_baseline_base,
    b.rc_grade_level_baseline_base,
    b.rc_learning_level_status_baseline_base,
    b.rc_endline_baseline_growth_base,
    -- -- Midline
    -- m.RC_level_mid,
    -- m.RC_grade_level_mid,
    -- m.RC_status_mid,
    -- m.RC_assessed_perc_mid,
    -- -- Endline
    -- e.RC_level_end,
    -- e.RC_grade_level_end,
    -- e.RC_status_end,
    -- e.RC_assessed_perc_end,
    
    -- Reading Fundamentals (RF) Metrics
    -- Baseline
    b.rf_level_baseline_base,
    b.rf_baseline_growth_base,
    -- -- Midline
    -- m.RF_status_mid,
    -- m.RF_perc_mid,
    -- m.RF_code_mid,
    -- -- Endline
    -- e.RF_status_end,
    -- e.RF_perc_end,
    -- e.RF_code_end,
    
    -- Mathematics Core Metrics
    -- Baseline
    b.math_level_baseline_base,
    b.math_baseline_grade_base,
    b.math_learning_level_status_baseline_base,
    b.math_endline_baseline_growth_base,
    -- -- Midline
    -- m.math_level_mid,
    -- m.math_status_mid,
    -- m.math_mastery_mid,
    -- -- Endline
    -- e.math_level_end,
    -- e.math_status_end,
    -- e.math_mastery_end,
    
    -- Mathematics Subject-wise Scores
    -- Baseline
    b.final_baseline_level_mastery_base,
    b.baseline_numbers_base,
    b.baseline_patterns_base,
    b.baseline_geometry_base,
    b.baseline_total_in_mensuration_base,
    b.baseline_total_in_time_base,
    b.baseline_total_in_operations_base,
    b.baseline_total_in_data_base,
    -- -- Midline
    -- m.math_perc_numbers_mid,
    -- m.math_perc_patterns_mid,
    -- m.math_perc_geometry_mid,
    -- m.math_perc_mensuration_mid,
    -- m.math_perc_time_mid,
    -- m.math_perc_operations_mid,
    -- m.math_perc_data_handling_mid,
    -- -- Endline
    -- e.math_perc_numbers_end,
    -- e.math_perc_patterns_end,
    -- e.math_perc_geometry_end,
    -- e.math_perc_mensuration_end,
    -- e.math_perc_time_end,
    -- e.math_perc_operations_end,
    -- e.math_perc_data_handling_end,
    
    -- Reading Comprehension Components
    -- Baseline
    b.baseline_factual_base,
    b.baseline_inference_base,
    b.baseline_critical_thinking_base,
    -- -- Midline
    -- m.factual_mid,
    -- m.inference_mid,
    -- m.critical_thinking_mid,
    -- -- Endline
    -- e.factual_end,
    -- e.inference_end,
    -- e.critical_thinking_end,
    
    -- Language Skills
    -- Baseline
    b.baseline_vocabulary_base,
    b.baseline_grammar_base,
    -- -- Midline
    -- m.vocabulary_mid,
    -- m.grammar_mid,
    -- -- Endline
    -- e.vocabulary_end,
    -- e.grammar_end,
    
    b.baseline_assessed_percentage_base,

    -- RF Skills
    -- Baseline
    b.baseline_letter_sounds_base,
    b.baseline_cvc_words_base,
    b.baseline_blends_base,
    b.baseline_consonant_diagraph_base,
    b.baseline_magic_e_words_base,
    b.baseline_vowel_diagraphs_base,
    b.baseline_multi_syllabelle_words_base,
    b.baseline_passage_1_base,
    b.baseline_passage_2_base
    -- -- Midline
    -- m.letter_sounds_mid,
    -- m.CVC_words_mid,
    -- m.blends_mid,
    -- m.consonant_diagraph_mid,
    -- m.magic_E_words_mid,
    -- m.vowel_diagraphs_mid,
    -- m.multi_syllabelle_words_mid,
    -- m.passage_1_mid,
    -- m.passage_2_mid,
    -- -- Endline
    -- e.letter_sounds_end,
    -- e.CVC_words_end,
    -- e.blends_end,
    -- e.consonant_diagraph_end,
    -- e.magic_E_words_end,
    -- e.vowel_diagraphs_end,
    -- e.multi_syllabelle_words_end,
    -- e.passage_1_end,
    -- e.passage_2_end,
    
    -- -- RF Proficiency Level Distribution
    -- -- Baseline
    -- b.developing_base,
    -- b.beginner_base,
    -- b.intermediate_base,
    -- b.advanced_base,
    -- -- Midline
    -- m.developing_mid,
    -- m.beginner_mid,
    -- m.intermediate_mid,
    -- m.advanced_mid,
    -- -- Endline
    -- e.developing_end,
    -- e.beginner_end,
    -- e.intermediate_end,
    -- e.advanced_end
from {{ref('base_mid_end_comb_students_25_26_dim')}} d
left join {{ref('baseline_25_26_stg')}} b 
    on d.student_id = b.student_id_base
-- left join {{ref('midline_2425_stg')}} m 
--     on d.student_id = m.student_id_mid
-- left join {{ref('endline_2425_stg')}} e 
--     on d.student_id = e.student_id_end
