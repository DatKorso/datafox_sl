type MatchLevel = 'season' | 'gender' | 'size' | 'last' | 'item';

export const RECOMMENDATIONS_CONFIG = {
  MIN_RECOMMENDATIONS: 8,
  MAX_RECOMMENDATIONS: 8,
  SCORING: {
    BASE_SCORE: 100,
    EXACT_SEASON: 50,
    EXACT_GENDER: 50,
    EXACT_MATERIAL: 25,
    EXACT_FASTENER: 25,
    EXACT_SIZE: 60,
    LARGER_SIZE: 5,
    EXACT_MEGA_LAST: 50,
    EXACT_NEW_LAST: 20,
    EXACT_BEST_LAST: 30,
    EXACT_ITEM: 50,
  },
  MATCH_LEVELS: [
    ['season', 'gender', 'size', 'last', 'item'],     // Уровень 1: все параметры
    ['season', 'gender', 'size', 'item'],             // Уровень 2: без колодки
    ['season', 'gender', 'item'],                     // Уровень 3: без размера
    ['season', 'gender'],                             // Уровень 4: только сезон и пол
    ['gender'],                                       // Уровень 5: только пол
    [],                                                // Уровень 6: без ограничений
  ] as Array<Array<MatchLevel>>,
} as const; 