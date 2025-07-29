# WB Recommendations Algorithm Improvement

## Problem Description
Users reported that even when increasing the minimum recommendation limit in the WB recommendations page, the final table still showed only 5 recommendations per article instead of the requested amount.

## Root Cause Analysis
The issue was in the status logic of the WB recommendation system:

1. **Status Logic Bug**: Products with fewer than `min_recommendations` were marked as `INSUFFICIENT_RECOMMENDATIONS`
2. **Success Property**: Only products with `WBProcessingStatus.SUCCESS` had `success = True`
3. **UI Filter**: The results table only displayed products where `result.success == True`

## Implemented Solutions

### 1. Fixed Status Logic (`utils/wb_recommendations.py:346-348`)
```python
@property
def success(self) -> bool:
    """Успешность обработки"""
    return self.status in [WBProcessingStatus.SUCCESS, WBProcessingStatus.INSUFFICIENT_RECOMMENDATIONS]
```

**Before**: Only `SUCCESS` status was considered successful
**After**: Both `SUCCESS` and `INSUFFICIENT_RECOMMENDATIONS` are treated as successful results

### 2. Enhanced Fallback Algorithm (`utils/wb_recommendations.py:1037-1063`)
Added intelligent fallback logic when insufficient high-quality matches are found:

- **Dynamic Threshold**: Reduces score threshold by 20 points when needed
- **Duplicate Prevention**: Ensures no duplicate candidates are added
- **Capacity Limit**: Respects `max_recommendations` setting
- **Logging**: Provides detailed feedback about fallback activation

### 3. Improved Configuration Defaults (`utils/wb_recommendations.py:287-289`)
```python
max_recommendations: int = 20
min_recommendations: int = 5  # Для предупреждения, не блокировки
min_score_threshold: float = 40.0  # Снижен для увеличения рекомендаций
```

### 4. UI Configuration Updates (`pages/16_🎯_Рекомендации_WB.py:142-152`)
- Updated help text to clarify recommended values
- Increased max range for `min_recommendations` slider (1-25)
- Added guidance for optimal settings

## Algorithm Enhancement Details

### Optional Similarity Fields Optimization
The following fields now contribute more effectively to recommendations:
- `heel_type_match_bonus: 50`
- `sole_type_match_bonus: 50` 
- `heel_up_type_match_bonus: 50`
- `lacing_type_match_bonus: 50`
- `nose_type_match_bonus: 50`

### Fallback Logic Flow
1. **Primary Filter**: Apply standard `min_score_threshold` (40.0)
2. **Evaluation**: Check if recommendations >= `min_recommendations`
3. **Fallback Trigger**: If insufficient, reduce threshold to `max(20.0, threshold - 20.0)`
4. **Secondary Search**: Find additional candidates with lower scores
5. **Final Sort**: Order all recommendations by score descending

## Expected Results
- **Immediate**: Users will see up to 20 recommendations per product (respecting max_recommendations setting)
- **Quality**: Fallback ensures sufficient recommendations while maintaining quality standards
- **Transparency**: Enhanced logging provides insight into algorithm behavior
- **Flexibility**: UI configuration allows fine-tuning for different use cases

## Testing Recommendations
1. Test with products that previously showed only 5 recommendations
2. Verify score distribution in results (should see more variety)
3. Check fallback activation in logs for products with limited matches
4. Validate that high-quality products still get premium recommendations

## Backward Compatibility
All changes maintain backward compatibility through:
- Configuration-driven behavior
- Default values that work with existing data
- No breaking changes to API interfaces