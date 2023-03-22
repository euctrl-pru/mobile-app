-- Save this code in a file named "carousel.lua" in your "_quarto" folder
function create_carousel(images)
  local carousel_id = "carouselExampleIndicators"

  local indicators = {}
  for i, img in ipairs(images) do
    local active = i == 1 and " active" or ""
    table.insert(indicators, string.format('<li data-bs-target="#%s" data-bs-slide-to="%d" class="%s"></li>', carousel_id, i - 1, active))
  end

  local items = {}
  for i, img in ipairs(images) do
    local active = i == 1 and " active" or ""
    table.insert(items, string.format('<div class="carousel-item%s" data-bs-interval="10000000"><img class="d-block w-100" src="%s" alt="%s slide"></div>', active, img.src, img.alt))
  end

  return string.format([[
<div id="%s" class="carousel slide carousel-fade" data-bs-ride="carousel">
  <ol class="carousel-indicators">
    %s
  </ol>
  <div class="carousel-inner">
    %s
  </div>
</div>
]], carousel_id, table.concat(indicators, "\n"), table.concat(items, "\n"))
end

return create_carousel
