{%- macro pre_fact_operations(checks) -%}
    {%- set target_relation = this %} 
    {%- set existing_relation = load_relation(this) -%}
    {% if existing_relation is none%} 
    {% else %}
       {% if execute %}
            {{ checks }}
        {% endif %}
    {% endif %}
    
{%- endmacro -%}
