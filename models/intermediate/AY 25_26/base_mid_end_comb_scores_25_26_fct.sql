select
    d.student_id,
    
    -- Reading Comprehension (RC) Metrics
    -- Baseline
    b.rc_level_baseline_base,
    b.rc_grade_level_baseline_base,
    b.rc_learning_level_status_baseline_base,
    b.rc_endline_baseline_growth_base,
    -- Midline
    m.rc_level_midline_mid,
    m.rc_grade_level_midline_mid,
    m.rc_learning_level_status_midline_mid,
    m.rc_baseline_midline_growth_mid,
    m.rc_midline_growth_status_mid,
    -- Endline
    e.rc_level_endline_end,
    e.rc_grade_level_endline_end,
    e.rc_learning_level_status_endline_end,
    e.rc_midline_endline_growth_end,
    e.rc_endline_growth_status_end,
    
    -- Reading Fundamentals (RF) Metrics
    -- Baseline
    b.rf_level_baseline_base,
    b.rf_baseline_growth_base,
    -- Midline
    m.rf_level_midline_mid,
    m.rf_midline_growth_mid,
    -- Endline
    e.rf_level_endline_end,
    e.rf_endline_growth_end,
    
    -- Mathematics Core Metrics
    -- Baseline
    b.math_level_baseline_base,
    b.math_baseline_grade_base,
    b.math_learning_level_status_baseline_base,
    b.math_endline_baseline_growth_base,
    -- Midline
    m.math_level_midline_mid,
    m.math_midline_grade_mid,
    m.math_learning_level_status_midline_mid,
    m.math_baseline_midline_growth_mid,
    m.math_midline_growth_status_mid,
    -- -- Endline
    e.math_level_endline_end,
    e.math_endline_grade_end,
    e.math_learning_level_status_endline_end,
    e.math_midline_endline_growth_end,
    e.math_endline_growth_status_end,
    
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
    -- Midline
    m.final_midline_level_mastery_mid,
    m.midline_numbers_mid,
    m.midline_patterns_mid,
    m.midline_geometry_mid,
    m.midline_total_in_mensuration_mid,
    m.midline_total_in_time_mid,
    m.midline_total_in_operations_mid,
    m.midline_total_in_data_mid,
    -- Endline
    e.final_endline_level_mastery_end,
    e.endline_numbers_end,
    e.endline_patterns_end,
    e.endline_geometry_end,
    e.endline_total_in_mensuration_end,
    e.endline_total_in_time_end,
    e.endline_total_in_operations_end,
    e.endline_total_in_data_end,
    
    -- Reading Comprehension Components
    -- Baseline
    b.baseline_factual_base,
    b.baseline_inference_base,
    b.baseline_critical_thinking_base,
    -- Midline
    m.midline_factual_mid,
    m.midline_inference_mid,
    m.midline_critical_thinking_mid,
    -- Endline
    e.endline_factual_end,
    e.endline_inference_end,
    e.endline_critical_thinking_end,
    
    -- Language Skills
    -- Baseline
    b.baseline_vocabulary_base,
    b.baseline_grammar_base,
    -- Midline
    m.midline_vocabulary_mid,
    m.midline_grammar_mid,
    -- Endline
    e.endline_vocabulary_end,
    e.endline_grammar_end,
    
    b.baseline_assessed_percentage_base,
    m.midline_assessed_percentage_mid,
    e.endline_assessed_percentage_end,

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
    b.baseline_passage_2_base,
    -- Midline
    m.midline_letter_sounds_mid,
    m.midline_cvc_words_mid,
    m.midline_blends_mid,
    m.midline_consonant_diagraph_mid,
    m.midline_magic_e_words_mid,
    m.midline_vowel_diagraphs_mid,
    m.midline_multi_syllabelle_words_mid,
    m.midline_passage_1_mid,
    m.midline_passage_2_mid,
    -- Endline
    e.endline_letter_sounds_end,
    e.endline_cvc_words_end,
    e.endline_blends_end,
    e.endline_consonant_diagraph_end,
    e.endline_magic_e_words_end,
    e.endline_vowel_diagraphs_end,
    e.endline_multi_syllabelle_words_end,
    e.endline_passage_1_end,
    e.endline_passage_2_end

from {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
left join {{ ref('baseline_25_26_stg') }} as b 
    on d.student_id = b.student_id_base
left join {{ ref('midline_25_26_stg') }} as m 
    on d.student_id = m.student_id_mid
left join {{ ref('endline_25_26_stg') }} as e 
    on d.student_id = e.student_id_end
