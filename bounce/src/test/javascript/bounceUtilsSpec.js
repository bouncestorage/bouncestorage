describe("Test utils", function() {
  it("prints KB for kilobytes", function() {
    expect(BounceUtils.toHumanSize(3567)).toEqual("3.48 KB");
  });

  it("prints MB for megabytes", function() {
    expect(BounceUtils.toHumanSize(56432342)).toEqual("53.8 MB");
  });

  it("prints GB for gigabytes", function() {
    expect(BounceUtils.toHumanSize(342*1024*1024*1024)).toEqual("342  GB");
  });

  it("prints TB for terrabytes", function() {
    expect(BounceUtils.toHumanSize(8.57*1024*1024*1024*1024)).toEqual("8.57 TB");
  });
});
