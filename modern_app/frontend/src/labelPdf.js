import { jsPDF } from "jspdf";

export function createLabelPdf(images, settings) {
  const { width, height } = settings;
  const orientation = width >= height ? "landscape" : "portrait";
  const doc = new jsPDF({ unit: "in", format: [width, height], orientation, compress: true });
  doc.setProperties({ title: "Evidence Labels", creator: "CyberLab Case Tracker" });
  images.forEach((image, index) => {
    if (index) doc.addPage([width, height], orientation);
    doc.addImage(image, "PNG", 0, 0, width, height);
  });
  return doc;
}
