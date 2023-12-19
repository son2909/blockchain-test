export const isDateWithinLast7Days = (date: Date): boolean => {
  const sevenDaysInMillis: number = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds
  const currentDate: Date = new Date();
  return (currentDate.getTime() - date.getTime()) <= sevenDaysInMillis;
}