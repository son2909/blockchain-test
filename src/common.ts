export const isDateWithinLast7Days = (date: Date): boolean => {
  const sevenDaysInMillis: number = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds
  const currentDate: Date = new Date();
  return currentDate.getTime() - date.getTime() <= sevenDaysInMillis;
};

export const parsePostgresUri = (uri: string) => {
  const match = uri.match(
    /^postgresql:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/([^?]+)/,
  );

  if (!match) {
    console.error("Invalid PostgreSQL connection string format");
    return null;
  }

  const [, username, password, host, portStr, database] = match;

  return {
    type: "postgres",
    host,
    port: parseInt(portStr, 10),
    username,
    password,
    database,
  };
};
