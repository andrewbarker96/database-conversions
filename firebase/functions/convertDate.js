export function convertDateToIso(date) {
  if (!date) {
    console.warn("Missing Date Value:", { date });
    return;
  }

  try {
    const [month, day, year] = date.split("-");
    const formattedDate = `${year}-${month.padStart(2, "0")}-${day.padStart(
      2,
      "0"
    )}`;

    const isoDate = new Date(formattedDate).toISOString().split("T")[0];
    return isoDate;
  } catch (error) {
    console.error("Error converting date to ISO: ", { date, error });
    return;
  }
}
