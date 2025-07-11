export const config = {
  openAI: {
    baseURL: 'https://openrouter.ai/api/v1',
    model: 'deepseek/deepseek-chat',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer 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',
      'X-Title': 'Excel AI Processor',
    },
  },
  excel: {
    questionColumn: 'Вопрос',
    promptColumn: 'Промпт',
    answerColumn: 'Ответ',
  },
}; 